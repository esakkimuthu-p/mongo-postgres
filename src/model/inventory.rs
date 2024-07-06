use std::collections::HashSet;

use super::*;

pub struct Inventory;

impl Inventory {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let inventory_heads = mongodb
            .collection::<Document>("inventory_heads")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let units = mongodb
            .collection::<Document>("units")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1, "name": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let manufacturers = mongodb
            .collection::<Document>("manufacturers")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let pharma_salts = mongodb
            .collection::<Document>("pharma_salts")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1, "name": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let mut cur = mongodb
            .collection::<Document>("inventories")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 2;
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let inv_units = d.get_array_document("units").unwrap();
            let division = inventory_heads
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("head").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            let mut cess = None;
            if let Some(c) = d._get_document("cess") {
                if !c.is_empty() {
                    cess = Some(
                        serde_json::json!({ "on_value":c._get_f64("onValue"), "on_qty":  c._get_f64("onQty")}),
                    );
                }
            }
            let barcodes = d
                .get_array("barcodes")
                .map(|x| {
                    x.iter()
                        .map(|x| x.as_str().unwrap().to_string().clone())
                        .collect::<Vec<String>>()
                })
                .ok();
            let mut salts = Vec::new();
            for b in d.get_array("salts").unwrap_or(&vec![]) {
                let s = pharma_salts
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == b.as_object_id().unwrap())
                            .then_some(x._get_i32("postgres").unwrap())
                    })
                    .unwrap();
                salts.push(s);
            }
            let mut manufacturer = None;
            let mut manufacturer_name = None;
            if let Ok(id) = d.get_object_id("manufacturerId") {
                manufacturer = manufacturers.iter().find_map(|x| {
                    (x.get_object_id("_id").unwrap() == id)
                        .then_some(x._get_i32("postgres").unwrap())
                });
                manufacturer_name = d.get_str("manufacturerName").ok();
            }
            for u in inv_units {
                id += 1;
                let loose_qty = u._get_i32("conversion").unwrap();
                let mut name = d.get_string("name").unwrap();
                let (unit, unit_name) = units
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == u.get_object_id("unitId").unwrap())
                            .then_some((
                                x._get_i32("postgres").unwrap(),
                                x.get_str("name").unwrap(),
                            ))
                    })
                    .unwrap();
                if loose_qty != 1 {
                    name = format!("{} - {}", name, unit_name);
                }
                postgres
                .execute(
                    "INSERT INTO inventory 
                    (id,name, division_id, allow_negative_stock, gst_tax_id, unit_id, sale_unit_id, purchase_unit_id,cess,
                        purchase_config,sale_config, barcodes,hsn_code, description, manufacturer_id, manufacturer_name, 
                        salts,loose_qty
                    ) 
                    OVERRIDING SYSTEM VALUE VALUES 
                    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                    &[
                        &id,
                        &name,
                        &division,
                        &d.get_bool("allowNegativeStock").unwrap_or_default(),
                        &d.get_str("tax").unwrap(),
                        &unit,
                        &unit,
                        &unit,
                        &cess,
                        &serde_json::json!({"mrp_editable": true, "tax_editable": true, "free_editable": true, "disc_1_editable": true, "disc_2_editable": true, "p_rate_editable": true, "s_rate_editable": true}),
                        &serde_json::json!({"rate_editable": false, "tax_editable": false, "unit_editable": false, "disc_editable": false}),
                        &barcodes,
                        &d.get_str("hsnCode").ok(),
                        &d.get_str("description").ok(),
                        &manufacturer,
                        &manufacturer_name,
                        &(!salts.is_empty()).then_some(salts.clone()),
                        &loose_qty
                    ],
                )
                .await
                .unwrap();
                let mut b_ids = HashSet::new();
                for br_de in d
                    .get_array("branchDetails")
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|x| x.as_document().unwrap())
                {
                    let branch = branches.iter().find(|x| {
                        x.get_object_id("_id").unwrap() == br_de.get_object_id("branch").unwrap()
                    });
                    if let Some(br) = branch {
                        let branch_id = br._get_i32("postgres").unwrap();
                        if b_ids.insert(branch_id) {
                            let rack = br_de
                                ._get_document("rack")
                                .and_then(|x| x.get_string("displayName"));
                            let s_disc = br_de
                                ._get_document("sDisc")
                                .map(|x| serde_json::to_value(x).unwrap());
                            postgres
                            .execute(
                                "INSERT INTO inventory_branch_detail 
                                (inventory_id,inventory_name, branch_id, branch_name, inventory_barcodes, s_disc) 
                                VALUES 
                                ($1,$2,$3,$4,$5,$6::JSON)",
                                &[
                                    &id,
                                    &name,
                                    &branch_id,
                                    &br.get_str("name").unwrap(),
                                    &barcodes,
                                    &s_disc
                                ],
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
                updates.push(doc! {
                    "q": { "inventory": object_id, "looseQty": loose_qty },
                    "u": { "$set": { "postgres": id} },
                    "multi": true
                });
            }
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "closing_batches",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
