use crate::AppState;
use crate::data::PendingMigration;
use crate::error::TierError;
use axum::Json;
use axum::extract::State;

pub async fn get_migration_list(
    State(state): State<AppState>,
) -> Result<Json<Vec<PendingMigration>>, TierError> {
    let res = crate::migrate::get_pending_migrations(&state.config, state.db).await?;
    Ok(Json(res))
}
