//! STAC 1.0 API — search, collections, and items backed by the Mantle catalog.

mod filter;
mod items;
mod models;
mod routes;

pub use filter::StacSearchRequest;
pub use items::{build_item_collection, dataset_to_stac_item, datasets_to_stac_items};
pub use models::{
    collection_list, default_collection, landing_catalog, StacCatalog, StacCollection,
    StacCollectionList, StacItem, StacItemCollection, StacLink, DEFAULT_COLLECTION_ID,
};
pub use routes::{router, StacState};
