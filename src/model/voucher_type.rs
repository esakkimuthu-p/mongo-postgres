use super::*;

pub struct VoucherType;

impl VoucherType {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let base_types: [&str; 8] = [
            "PAYMENT",
            "RECEIPT",
            "CONTRA",
            "JOURNAL",
            "SALE",
            "PURCHASE",
            "CREDIT_NOTE",
            "DEBIT_NOTE",
        ];
        let mut cur = mongodb
            .collection::<Document>("voucher_types")
            .find(
                doc! {"voucherType": {"$in": base_types.to_vec() }},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 100;
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let config = match d.get_str("voucherType").unwrap() {
                "PAYMENT" => {
                    let mut type_id = 1;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!({"payment": {"print_after_save": false}})
                }
                "RECEIPT" => {
                    let mut type_id = 2;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!({"receipt": {"print_after_save": false}})
                }
                "CONTRA" => {
                    let mut type_id = 3;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!({ "contra": {"print_after_save": false }})
                }
                "JOURNAL" => {
                    let mut type_id = 4;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!({ "journal": {"print_after_save": false }})
                }
                "SALE" => {
                    let mut type_id = 5;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!(
                        {"sale": {"account": {"print_after_save": false}, "inventory": {"hide_rack": false, "tax_editable": false, "rate_editable": false, "unit_editable": false, "set_default_qty": false, "barcode_enabled": false, "print_after_save": false, "auto_select_batch": false, "default_priceList": null, "discount_editable": false, "warehouse_enabled": false, "price_list_editable": false, "enable_sales_person": false, "allow_credit_customer": false,  "hide_mrp_in_batch_modal": false, "set_focus_on_inventory": false, "bill_discount_editable": false,  "customer_form_quick_create": false, "voucherwise_sales_person": false, "freeze_sales_person_for_voucher": false}}}
                    )
                }
                "CREDIT_NOTE" => {
                    let mut type_id = 6;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!(
                        {"credit_note": {"account": {"print_after_save": false}, "inventory": {"enable_exp": false, "tax_editable": false, "rate_editable": false, "unit_editable": false, "barcode_enabled": false, "print_after_save": false, "discount_editable": false, "warehouse_enabled": false, "invoice_no_required": false, "enable_sales_person": false, "allow_credit_customer": false,  "bill_discount_editable": false, "customer_form_quick_create": false, "voucherwise_sales_person": false, "freeze_sales_person_for_voucher": false}}}
                    )
                }
                "PURCHASE" => {
                    let mut type_id = 7;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!(
                        {"purchase": {"account": {"print_after_save": false}, "inventory": {"tax_hide": false, "enable_gin": false, "s_sate_as_mrp": false, "prevent_loss": false, "barcode_enabled": false, "print_after_save": false, "s_rate_mrp_required": false, "allow_credit_vendor": true}}}
                    )
                }

                "DEBIT_NOTE" => {
                    let mut type_id = 8;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    updates.push(doc! {
                        "q": { "_id": object_id },
                        "u": { "$set": { "postgres": type_id} },
                    });
                    serde_json::json!(
                        {"debit_note": {"account": {"print_after_save": false}, "inventory": {"enable_exp": false, "tax_editable": false, "rate_editable": false, "barcode_enabled": false, "bill_no_required": false, "print_after_save": false, "discount_editable": false, "warehouse_enabled": false, "allow_credit_vendor": false}}}
                    )
                }
                _ => panic!("Invalid voucherTypes"),
            };
            if !d.get_bool("default").unwrap_or_default() {
                postgres
                    .execute(
                        "INSERT INTO voucher_type (id, name, base_type, config, prefix)
                     OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3::text, $4::json, $5)",
                        &[
                            &id,
                            &d.get_str("name").unwrap(),
                            &d.get_str("voucherType").unwrap(),
                            &config,
                            &d.get_str("prefix").ok(),
                        ],
                    )
                    .await
                    .unwrap();
            }
            let command = doc! {
                "update": "voucher_types",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
