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
                        "inventory": 1, "branch": 1, "sRate": 1, "mrp": 1, "pRate": 1,
                        "unit": {"$arrayElemAt": ["$unit.postgres", 0]},
                        "batchNo": 1, "expiry": 1,"avgNlc": 1,
                        "qty": {"$cond": [
                            { "$eq": [{ "$mod": [ { "$subtract": ["$inward", "$outward"] }, "$unitConv"] }, 0] },
                            {"$divide": [ { "$subtract": ["$inward", "$outward"] }, "$unitConv"]},
                            { "$subtract": ["$inward", "$outward"] }
                        ]},
                        "looseQty": {"$cond": [
                            { "$eq": [{ "$mod": [ { "$subtract": ["$inward", "$outward"] }, "$unitConv"] }, 0] },
                            "$unitConv",
                            1
                        ]}
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
            .keys(doc! {"inventory":1, "looseQty": 1})
            .build();
        mongodb
            .collection::<Document>("closing_batches")
            .create_index(idx, None)
            .await
            .unwrap();
    }
    pub async fn opening(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("closing_batches")
            .aggregate(
                vec![
                    doc! {
                        "$group": {
                            "_id": {"branch": "$branch", "postgres": "$postgres"},
                            "inv_trns": {"$push": {
                               "qty": "$qty",
                               "nlc": {"$ifNull": ["$avgNlc", {"$ifNull": ["$pRate", 0.0]}]},
                               "cost": {"$ifNull": ["$avgNlc", {"$ifNull": ["$pRate", 0.0]}]},
                               "unit_id": "$unit",
                               "unit_conv": 1,
                               "loose_qty": "$looseQty",
                               "rate": {"$ifNull": ["$pRate", {"$ifNull": ["$avgNlc", 0.0]}]},
                               "batch_no": "$batchNo",
                               "mrp": "$mrp",
                               "s_rate": "$sRate",
                               "expiry": "$expiry",
                               "asset_amount":{"$round": [{"$multiply": ["$avgNlc", "$qty"]}, 2]}
                            }}
                        }
                    },
                    doc! {
                        "$project": {
                            "_id": 0,
                            "branch": "$_id.branch",
                            "inventory": "$_id.postgres",
                            "inv_trns": 1
                        }
                    },
                ],
                None,
            )
            .await
            .unwrap();
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let branch = branches
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            let trns = d
                .get_array("inv_trns")
                .unwrap()
                .iter()
                .map(|x| x.as_document().unwrap().clone())
                .collect::<Vec<Document>>();
            let data = serde_json::json!({
                "branch_id": branch,
                "warehouse_id": 1,
                "inventory_id":d._get_i32("inventory").unwrap(),
                "inv_items": &serde_json::to_value(trns).unwrap(),
            });
            postgres
                .execute("select * from set_inventory_opening($1::json)", &[&data])
                .await
                .unwrap();
        }
    }
}
