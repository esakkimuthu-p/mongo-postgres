use std::collections::HashSet;

use super::*;

pub struct Inventory;

impl Inventory {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        postgres
            .execute(
                "INSERT INTO price_list (id, name) overriding system value VALUES (1, 'main price list')",
                &[],
            )
            .await
            .unwrap();
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
        let sections = mongodb
            .collection::<Document>("sections")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let closing_batches = mongodb
            .collection::<Document>("closing_batches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 0, "inventory": 1, "unitConv": 1}, doc! {}),
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
        let racks = mongodb
            .collection::<Document>("racks")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "displayName": 1}, doc! {"_id": 1}),
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
            let mut category1 = None;
            if let Ok(id) = d.get_object_id("manufacturerId") {
                manufacturer = manufacturers.iter().find_map(|x| {
                    (x.get_object_id("_id").unwrap() == id)
                        .then_some(x._get_i32("postgres").unwrap())
                });
            }
            if let Ok(id) = d.get_object_id("sectionId") {
                category1 = sections.iter().find_map(|x| {
                    (x.get_object_id("_id").unwrap() == id)
                        .then_some(vec![x._get_i32("postgres").unwrap()])
                });
            }
            let primary_unit_id = inv_units
                .iter()
                .find_map(|x| {
                    (x._get_i32("conversion").unwrap() == 1)
                        .then_some(x.get_object_id("unitId").unwrap())
                })
                .unwrap();
            let primary_unit = units
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == primary_unit_id)
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            for u in inv_units {
                let loose_qty = u._get_i32("conversion").unwrap();
                let has_stock = closing_batches.iter().any(|x| {
                    x.get_object_id("inventory").unwrap() == object_id
                        && loose_qty == x._get_i32("unitConv").unwrap()
                });
                if has_stock {
                    let mut name = d.get_string("name").unwrap();
                    let unit_name = units
                        .iter()
                        .find_map(|x| {
                            (x.get_object_id("_id").unwrap() == u.get_object_id("unitId").unwrap())
                                .then_some(x.get_str("name").unwrap())
                        })
                        .unwrap();
                    if loose_qty != 1 {
                        name = format!("{} - {}", name, unit_name);
                    }
                    let id : i32 = postgres
                    .query_one(
                        "INSERT INTO inventory 
                        (name, division_id, allow_negative_stock, gst_tax_id, unit_id, sale_unit_id, purchase_unit_id,cess,
                            purchase_config,sale_config, barcodes,hsn_code, description, manufacturer_id, 
                            salts,loose_qty,category1) VALUES 
                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) returning id",
                        &[
                            &name,
                            &division,
                            &d.get_bool("allowNegativeStock").unwrap_or_default(),
                            &d.get_str("tax").unwrap(),
                            &primary_unit,
                            &primary_unit,
                            &primary_unit,
                            &cess,
                            &serde_json::json!({"mrp_editable": true, "tax_editable": true, "free_editable": true, "disc_1_editable": true, "disc_2_editable": true, "p_rate_editable": true, "s_rate_editable": true}),
                            &serde_json::json!({"rate_editable": false, "tax_editable": false, "unit_editable": false, "disc_editable": false}),
                            &barcodes,
                            &d.get_str("hsnCode").ok(),
                            &d.get_str("description").ok(),
                            &manufacturer,
                            &(!salts.is_empty()).then_some(salts.clone()),
                            &loose_qty,
                            &category1
                        ],
                    )
                    .await
                    .unwrap().get(0);
                    let mut b_ids = HashSet::new();
                    let mut disc_values = vec![];
                    for br_de in d
                        .get_array("branchDetails")
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|x| x.as_document().unwrap())
                    {
                        let branch = branches.iter().find(|x| {
                            x.get_object_id("_id").unwrap()
                                == br_de.get_object_id("branch").unwrap()
                        });
                        if let Some(br) = branch {
                            let branch_id = br._get_i32("postgres").unwrap();
                            if b_ids.insert(branch_id) {
                                let rack_id = br_de
                                    ._get_document("rack")
                                    .and_then(|x| x.get_object_id("id").ok());
                                let mut rack_name = None;
                                if let Some(rac) = rack_id {
                                    rack_name = racks.iter().find_map(|x| {
                                        (x.get_object_id("_id").unwrap() == rac)
                                            .then_some(x.get_string("displayName").unwrap())
                                    });
                                }
                                if let Some(s_disc) = br_de
                                    ._get_document("sDisc")
                                    .and_then(|x| x._get_f64("amount"))
                                {
                                    disc_values.push(s_disc);
                                }
                                postgres
                                .execute(
                                    "INSERT INTO inventory_branch_detail 
                                    (inventory_id,inventory_name, branch_id, branch_name, inventory_barcodes, stock_location_id) 
                                    VALUES 
                                    ($1,$2,$3,$4,$5,$6)",
                                    &[
                                        &id,
                                        &name,
                                        &branch_id,
                                        &br.get_str("name").unwrap(),
                                        &barcodes,
                                        &rack_name
                                    ],
                                )
                                .await
                                .unwrap();
                            }
                        }
                    }
                    updates.push(doc! {
                        "q": { "inventory": object_id, "unitConv": loose_qty },
                        "u": { "$set": { "postgres": id, "postgres_unit": primary_unit} },
                        "multi": true
                    });
                    if !(disc_values.is_empty()) {
                        let x = disc_values.into_iter().fold(f64::NEG_INFINITY, f64::max);
                        postgres
                            .execute(
                                "INSERT INTO price_list_condition 
                                    (apply_on, computation, price_list_id, value, inventory_id) 
                                    VALUES 
                                    ('INVENTORY','DISCOUNT',1,$1,$2)",
                                &[&x, &id],
                            )
                            .await
                            .unwrap();
                    }
                }
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
