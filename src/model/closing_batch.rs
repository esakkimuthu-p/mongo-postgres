use mongodb::IndexModel;

use super::*;

pub struct InventoryBranchBatch;

impl InventoryBranchBatch {
    pub async fn create(mongodb: &Database) {
        let _ = mongodb
            .collection::<Document>("batches")
            .aggregate(vec![
                doc!{ "$match": { "$expr" : { "$ne" : [ "$inward", "$outward" ] }}},
                doc!{"$lookup": {
                    "from": "units",
                    "localField": "unitId",
                    "foreignField": "_id",
                    "as": "unit"
                }},
                doc!{
                    "$project": {
                        "closing": { "$subtract": ["$inward", "$outward"] },
                        "inventory": 1, "branch": 1, "sRate": {"$round": ["$sRate", 2]}, "mrp": {"$round": ["$mrp", 2]}, "pRate": {"$round": ["$pRate", 2]},
                        "unit": {"$arrayElemAt": ["$unit.postgres", 0]},
                        "batchNo": 1, "expiry": 1,"avgNlc": 1,
                        "qty": { "$round": [{ "$divide": [{ "$subtract": ["$inward", "$outward"] }, "$unitConv"] }, 4] },
                        "is_loose_qty": { "$cond": [{ "$gt": ["$unitConv", 1] }, false, true] },
                        "unitConv":1,
                    }
                },
                doc!{ "$out": "closing_batches"}
            ], None)
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let idx = IndexModel::builder()
            .keys(doc! {"inventory":1, "unitConv": 1})
            .build();
        mongodb
            .collection::<Document>("closing_batches")
            .create_index(idx, None)
            .await
            .unwrap();
    }
    pub async fn opening(mongodb: &Database, postgres: &PostgresClient) {
        mongodb
            .collection::<Document>("closing_batches")
            .aggregate(
                vec![
                    doc! {
                        "$group": {
                            "_id": {"branch": "$branch", "postgres": "$postgres"},
                            "inv_items": {"$push": {
                               "qty": "$qty",
                               "nlc": {"$ifNull": ["$avgNlc", {"$ifNull": ["$pRate", 0.0]}]},
                               "cost": {"$ifNull": ["$avgNlc", {"$ifNull": ["$pRate", 0.0]}]},
                               "unit_id": "$unit",
                               "unit_conv": 1,
                               "is_loose_qty": "$is_loose_qty",
                               "rate": {"$ifNull": ["$pRate", {"$ifNull": ["$avgNlc", 0.0]}]},
                               "batch_no": "$batchNo",
                               "mrp": "$mrp",
                               "s_rate": "$sRate",
                               "expiry": "$expiry",
                               "asset_amount":{"$round": [{"$multiply": ["$avgNlc", "$qty"]}, 2]}
                            }}
                        }
                    },
                    doc! {"$lookup": {
                        "from": "branches",
                        "localField": "_id.branch",
                        "foreignField": "_id",
                        "as": "br"
                    }},
                    doc! {
                        "$project": {
                            "_id": 0,
                            "branch_id": {"$arrayElemAt": ["$br.postgres", 0]},
                            "inventory_id": "$_id.postgres",
                            "warehouse_id": {"$literal": 1},
                            "inv_items": 1
                        }
                    },
                    doc! { "$out": "inv_opening"},
                ],
                None,
            )
            .await
            .unwrap();
        let mut cur = mongodb
            .collection::<Document>("inv_opening")
            .find(doc! {}, None)
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let data = &serde_json::to_value(d).unwrap();
            postgres
                .execute("select * from set_inventory_opening($1::json)", &[&data])
                .await
                .unwrap();
        }
    }
}
