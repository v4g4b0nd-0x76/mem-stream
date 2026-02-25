use std::sync::Arc;

use axum::{Router, routing::{delete, get, post}};
use tower_http::cors::{Any, CorsLayer};

use crate::{api::handlers::*, client::pool::ClientPool};

pub fn build_router(pool: Arc<ClientPool> ) -> Router {
    let health_route= Router::new().route("/health" , get(health));
    let api_router = Router::new()
        .route("/groups", get(list_groups))
        .route("/groups/{name}", post(create_group))
        .route("/groups/{name}", delete(drop_group))
        .route("/groups/{name}/stats", get(group_stats))
        // Entry operations
        .route("/groups/{name}/entries", post(add_entry))
        .route("/groups/{name}/entries", get(read_range_entries))
        .route("/groups/{name}/entries", delete(drop_entires))
        .route("/groups/{name}/entries/batch", post(add_range_entries))
        .route("/groups/{name}/entries/{id}", get(read_entry)).with_state(pool);

    Router::new().merge(health_route).merge(api_router).layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
}