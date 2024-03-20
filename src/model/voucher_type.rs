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
                    serde_json::json!({"payment": {"printAfterSave": false}})
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
                    serde_json::json!({"receipt": {"printAfterSave": false}})
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
                    serde_json::json!({ "contra": {"printAfterSave": false }})
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
                    serde_json::json!({ "journal": {"printAfterSave": false }})
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
                        {"sale": {"account": {"printAfterSave": false}, "inventory": {"hideRack": false, "taxEditable": false, "rateEditable": false, "unitEditable": false, "printTemplate": {"enableSilentPrintMode": false}, "setDefaultQty": false, "barcodeEnabled": false, "printAfterSave": false, "autoSelectBatch": false, "defaultPriceList": null, "discountEditable": false, "warehouseEnabled": false, "priceListEditable": false, "enableSaleIncharge": false, "allowCreditCustomer": false, "cashRegisterEnabled": false, "hideMrpInBatchModal": false, "setFocusOnInventory": false, "billDiscountEditable": false, "allowedCreditAccounts": null, "customerFormQuickCreate": false, "voucherwiseSaleIncharge": false, "freezeSaleInchargeForVoucher": false}}}
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
                        {"creditNote": {"account": {"printAfterSave": false}, "inventory": {"enableExp": false, "taxEditable": false, "rateEditable": false, "unitEditable": false, "printTemplate": { "enableSilentPrintMode": false}, "barcodeEnabled": false, "printAfterSave": false, "discountEditable": false, "warehouseEnabled": false, "invoiceNoRequired": false, "enableSaleIncharge": false, "allowCreditCustomer": false, "cashRegisterEnabled": false, "billDiscountEditable": false, "customerFormQuickCreate": false, "voucherwiseSaleIncharge": false, "freezeSaleInchargeForVoucher": false}}}
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
                        {"purchase": {"account": {"printAfterSave": false}, "inventory": {"taxHide": false, "enableGin": false, "sRateAsMrp": false, "preventLoss": false, "printTemplate": { "enableSilentPrintMode": false}, "barcodeEnabled": false, "printAfterSave": false, "sRateMrpRequired": false, "allowCreditVendor": false}}}
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
                        {"debitNote": {"account": {"printAfterSave": false}, "inventory": {"enableExp": false, "taxEditable": false, "rateEditable": false, "printTemplate": {"enableSilentPrintMode": false}, "barcodeEnabled": false, "billNoRequired": false, "printAfterSave": false, "discountEditable": false, "warehouseEnabled": false, "allowCreditVendor": false, "cashRegisterEnabled": false}}}
                    )
                }
                _ => panic!("Invalid voucherTypes"),
            };
            if !d.get_bool("default").unwrap_or_default() {
                postgres
                .execute(
                    "INSERT INTO voucher_types (id, name, base_type, config, prefix)
                     OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3::TEXT::typ_base_voucher_type, $4::json, $5)",
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
