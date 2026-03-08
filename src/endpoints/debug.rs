use crate::AppState;
use crate::error::TierError;
use crate::migrate::MigrateAction;
use axum::Json;
use axum::extract::State;

pub async fn get_migration_list(
    State(state): State<AppState>,
) -> Result<Json<Vec<MigrateAction>>, TierError> {
    let res = crate::migrate::get_pending_migrations(&state.config, state.db).await?;
    Ok(Json(res))
}
