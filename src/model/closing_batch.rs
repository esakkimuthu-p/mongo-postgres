use super::*;

pub struct InventoryBranchBatch;

impl InventoryBranchBatch {
    pub async fn create(mongodb: &Database) {
        let _branches = mongodb
            .collection::<Document>("batches")
            .aggregate(vec![
                doc!{ "$match": { "$expr" : { "$ne" : [ "$inward", "$outward" ] }}},
                doc!{
                    "$project": {
                        "closing": { "$subtract": ["$inward", "$outward"] },
                        "qty": {"$divide": [ { "$subtract": ["$inward", "$outward"] }, "$unitConv"]},
                        "inventory": 1, "branch": 1, "sRate": 1, "mrp": 1, "pRate": 1,
                        "looseQty": "$unitConv", "unit": 1,"batchNo": 1, "expiry": 1,"avgNlc": 1
                    }
                },
                doc!{ "$out": "closing_batches"}
            ], None)
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
    }
}
