use async_trait::async_trait;
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;
use std::collections::BTreeMap;
use tracing::info;

use crate::controller::child_resources::delete_odoo_user_secret;
use crate::controller::helpers::{cron_depl_name, FIELD_MANAGER};
use crate::controller::state_machine::scale_deployment;
use crate::crd::odoo_instance::{OdooInstance, OdooInstanceStatus};
use crate::error::Result;

use super::{Context, ReconcileSnapshot, State};

/// Annotation a caller sets to ask for a rotation of the database password.
///
/// The operator reacts only to a value it has not recorded yet: a fresh opaque
/// token triggers exactly one rotation, and leaving the same value in place
/// never rotates twice. Nothing is derived from the token itself.
pub const ANNOTATION_ROTATE_DB_PASSWORD: &str = "odoo.bemade.org/rotate-db-password";

/// Running: steady state.  Ensures the Deployment replica count matches
/// `spec.replicas` so that live scaling (without a phase transition) works.
pub struct Running;

#[async_trait]
impl State for Running {
    async fn ensure(
        &self,
        instance: &OdooInstance,
        ctx: &Context,
        snap: &ReconcileSnapshot,
    ) -> Result<()> {
        let name = instance.metadata.name.as_deref().unwrap_or_default();
        let ns = instance.metadata.namespace.as_deref().unwrap_or_default();

        // Database password rotation. Deleting `<name>-odoo-user` is the whole
        // rotation: the ordinary reconcile path recreates it with a fresh
        // password, pushes it to PostgreSQL and re-renders odoo.conf, whose hash
        // rolls both Deployments. Doing it here, in Running, keeps the rotation
        // out of Initializing/Restoring/Upgrading, where the Secret is also in
        // use by a Job. The token is recorded in status so a re-reconcile of the
        // same annotation value never rotates a second time.
        let requested_token = pending_rotation_token(
            instance.metadata.annotations.as_ref(),
            instance.status.as_ref(),
        );
        if let Some(token) = requested_token {
            info!(%name, "rotating database password on request");
            delete_odoo_user_secret(&ctx.client, ns, name).await?;
            let api: Api<OdooInstance> = Api::namespaced(ctx.client.clone(), ns);
            api.patch_status(
                name,
                &PatchParams::apply(FIELD_MANAGER),
                &Patch::Merge(&json!({
                    "status": { "dbPasswordRotatedToken": token }
                })),
            )
            .await?;
            // Return without scaling: the Secret must exist again before the
            // Deployments roll onto the new credentials. The next tick does it.
            return Ok(());
        }

        let desired = instance.spec.replicas;

        if snap.deployment_replicas != desired {
            info!(%name, from = snap.deployment_replicas, to = desired, "scaling deployment");
            scale_deployment(&ctx.client, name, ns, desired).await?;
        }

        let desired = instance.spec.cron.replicas;
        let cron_depl_name = cron_depl_name(instance);
        info!(
            desired = desired,
            actual = snap.cron_deployment_replicas,
            "cron replicas while reconciling running state."
        );
        if snap.cron_deployment_replicas != desired {
            info!(%cron_depl_name, from = snap.cron_deployment_replicas, to = desired, "scaling cron deployment");
            scale_deployment(&ctx.client, cron_depl_name.as_str(), ns, desired).await?;
        }
        Ok(())
    }
}

/// Return the rotation token when the annotation asks for a rotation this
/// operator has not performed yet, `None` otherwise.
///
/// Pure on purpose: this decision is the only part worth testing without a
/// cluster, and getting it wrong either rotates on every reconcile or never
/// rotates at all.
fn pending_rotation_token(
    annotations: Option<&BTreeMap<String, String>>,
    status: Option<&OdooInstanceStatus>,
) -> Option<String> {
    let requested = annotations?.get(ANNOTATION_ROTATE_DB_PASSWORD)?;
    let rotated = status.and_then(|s| s.db_password_rotated_token.as_deref());
    (rotated != Some(requested.as_str())).then(|| requested.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn annotations(value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(ANNOTATION_ROTATE_DB_PASSWORD.to_string(), value.to_string())])
    }

    #[test]
    fn no_annotation_never_rotates() {
        assert_eq!(pending_rotation_token(None, None), None);
        let other = BTreeMap::from([("unrelated".to_string(), "x".to_string())]);
        assert_eq!(pending_rotation_token(Some(&other), None), None);
    }

    #[test]
    fn fresh_annotation_triggers_one_rotation() {
        let a = annotations("token-1");
        assert_eq!(
            pending_rotation_token(Some(&a), None),
            Some("token-1".to_string())
        );
    }

    #[test]
    fn already_rotated_token_does_not_trigger_again() {
        let a = annotations("token-1");
        let status = OdooInstanceStatus {
            db_password_rotated_token: Some("token-1".to_string()),
            ..Default::default()
        };
        assert_eq!(pending_rotation_token(Some(&a), Some(&status)), None);
    }

    #[test]
    fn a_new_token_triggers_a_second_rotation() {
        let a = annotations("token-2");
        let status = OdooInstanceStatus {
            db_password_rotated_token: Some("token-1".to_string()),
            ..Default::default()
        };
        assert_eq!(
            pending_rotation_token(Some(&a), Some(&status)),
            Some("token-2".to_string())
        );
    }
}
