use crate::AppState;
use crate::error::TierError;
use axum::extract::State;

pub async fn get_migration_list(State(state): State<AppState>) -> Result<(), TierError> {
    Ok(())
}
