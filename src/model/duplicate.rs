use super::*;

pub async fn duplicate_fix(db: &Database) {
    for collection in [
        "accounts",
        "inventories",
        "branches",
        "doctors",
        "pharma_salts",
        "units",
        "voucher_types",
        "sections",
        "manufacturers",
        "sale_incharges",
    ] {
        println!("{} duplicate fix start", collection);
        let docs = db
            .collection::<Document>(collection)
            .aggregate(
                vec![
                    doc! {"$group": {
                        "_id":"$validateName",
                        "ids": { "$addToSet": "$_id" }
                    }},
                    doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                    doc! { "$match": { "dup": true }},
                    doc! { "$project": { "ids": 1, "_id": 0 } },
                ],
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let mut updates = Vec::new();
        for duplicates in docs {
            for (idx, dup_id) in duplicates
                .get_array("ids")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_object_id().unwrap_or_default())
                .enumerate()
            {
                if idx != 0 {
                    updates.push(doc! {
                    "q": { "_id": dup_id },
                    "u": [
                        {"$set": {"name": {"$concat": ["$name", format!("Dup{}", idx),]},
                        "validateName": {"$concat": ["$validateName", format!("dup{}", idx),]},
                        "displayName": {"$concat": ["$displayName", format!("Dup{} ", idx),]}}}
                        ]
                    });
                }
            }
        }
        println!("count, {}", &updates.len());
        if !updates.is_empty() {
            let command = doc! {
                "update": collection,
                "updates": &updates
            };
            db.run_command(command, None).await.unwrap();
        }
        println!("{} duplicate fix end", collection);
    }
    println!("patients duplicate fix start");
    let docs = db
        .collection::<Document>("patients")
        .aggregate(
            vec![
                doc! {"$group": {
                    "_id": { "validateName": "$validateName", "customer": "$customer" },
                    "ids": { "$addToSet": "$_id" }
                }},
                doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                doc! { "$match": { "dup": true }},
                doc! { "$project": { "ids": 1, "_id": 0 } },
            ],
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut updates = Vec::new();
    for duplicates in docs {
        for (idx, dup_id) in duplicates
            .get_array("ids")
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_object_id().unwrap_or_default())
            .enumerate()
        {
            if idx != 0 {
                updates.push(doc! {"q": { "_id": dup_id }, "u": [
                    {"name": {"$concat": ["$name", format!("Dup{}", idx),]},
                    "validateName": {"$concat": ["$validateName", format!("dup{}", idx),]},
                    "displayName": {"$concat": ["$displayName", format!("Dup{} ", idx),]}}
                    ]
                });
            }
        }
    }
    println!("count, {}", &updates.len());
    if !updates.is_empty() {
        let command = doc! {
            "update": "patients",
            "updates": &updates
        };
        db.run_command(command, None).await.unwrap();
    }
    println!("patients duplicate fix end");

    println!("contacts duplicate fix start");
    let docs = db
        .collection::<Document>("contacts")
        .aggregate(
            vec![
                doc! {"$group": {
                    "_id": { "validateName": "$validateName", "mob": "$contactInfo.mobile" },
                    "ids": { "$addToSet": "$_id" }
                }},
                doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                doc! { "$match": { "dup": true }},
                doc! { "$project": { "ids": 1, "_id": 0 } },
            ],
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut updates = Vec::new();
    for duplicates in docs {
        for (idx, dup_id) in duplicates
            .get_array("ids")
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_object_id().unwrap_or_default())
            .enumerate()
        {
            if idx != 0 {
                updates.push(doc! {
                "q": { "_id": dup_id },
                "u": [{"$set":
                    {"name": {"$concat": ["$name", format!("Dup{}", idx),]},
                     "validateName": {"$concat": ["$validateName", format!("dup{}", idx),]},
                     "displayName": {"$concat": ["$displayName", format!("Dup{} ", idx),]}}
                     }]});
            }
        }
    }
    println!("count, {}", &updates.len());
    if !updates.is_empty() {
        let command = doc! {
            "update": "contacts",
            "updates": &updates
        };
        db.run_command(command, None).await.unwrap();
    }
    println!("contacts duplicate fix end");

    println!("batches duplicate fix start");
    let docs = db
        .collection::<Document>("batches")
        .aggregate(
            vec![
                doc! {"$group": {
                    "_id": { "batchNo": "$batchNo", "inventory": "$inventory", "branch": "$branch" },
                    "ids": { "$addToSet": "$_id" }
                }},
                doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                doc! { "$match": { "dup": true }},
                doc! { "$project": { "ids": 1, "_id": 0 } },
            ],
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut updates = Vec::new();
    for duplicates in docs {
        for (idx, dup_id) in duplicates
            .get_array("ids")
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_object_id().unwrap_or_default())
            .enumerate()
        {
            if idx != 0 {
                updates.push(doc! {"q": { "_id": dup_id }, "u": [{"$set": {"batchNo": {"$concat": ["$batchNo", format!("DUP{}", idx),]}} }]});
            }
        }
    }
    println!("count, {}", &updates.len());
    if !updates.is_empty() {
        let command = doc! {
            "update": "batches",
            "updates": &updates
        };
        db.run_command(command, None).await.unwrap();
    }
    println!("batches duplicate fix end");

    println!("member duplicate fix start");
    let docs = db
        .collection::<Document>("members")
        .aggregate(
            vec![
                doc! {"$group": {
                    "_id": "$username",
                    "ids": { "$addToSet": "$_id" }
                }},
                doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                doc! { "$match": { "dup": true }},
                doc! { "$project": { "ids": 1, "_id": 0 } },
            ],
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut updates = Vec::new();
    for duplicates in docs {
        for (idx, dup_id) in duplicates
            .get_array("ids")
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_object_id().unwrap_or_default())
            .enumerate()
        {
            if idx != 0 {
                updates.push(doc! {"q": { "_id": dup_id }, "u": [{"$set": {"username": {"$concat": ["$username", format!("dup{}", idx)]}} }]});
            }
        }
    }
    println!("count, {}", &updates.len());
    if !updates.is_empty() {
        let command = doc! {
            "update": "members",
            "updates": &updates
        };
        db.run_command(command, None).await.unwrap();
    }
    println!("members duplicate fix end");

    println!("print_templates duplicate fix start");
    let docs = db
        .collection::<Document>("print_templates")
        .aggregate(
            vec![
                doc! {"$group": {
                    "_id": "$name",
                    "ids": { "$addToSet": "$_id" }
                }},
                doc! { "$project": { "ids": 1, "dup": { "$gt": [{ "$size": "$ids" }, 1] } } },
                doc! { "$match": { "dup": true }},
                doc! { "$project": { "ids": 1, "_id": 0 } },
            ],
            None,
        )
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut updates = Vec::new();
    for duplicates in docs {
        for (idx, dup_id) in duplicates
            .get_array("ids")
            .unwrap_or(&vec![])
            .iter()
            .map(|x| x.as_object_id().unwrap_or_default())
            .enumerate()
        {
            if idx != 0 {
                updates.push(doc! {"q": { "_id": dup_id }, "u": [{"$set": {"name": {"$concat": ["$name", format!("dup{}", idx)]}} }]});
            }
        }
    }
    println!("count, {}", &updates.len());
    if !updates.is_empty() {
        let command = doc! {
            "update": "print_templates",
            "updates": &updates
        };
        db.run_command(command, None).await.unwrap();
    }
    db
    .collection::<Document>("voucher_types")
    .update_many(
        doc!{"voucherType": {"$in": ["MANUFACTURING_JOURNAL", "MATERIAL_CONVERSION", "STOCK_TRANSFER"]}}, 
        vec![doc! {"$set": {"default": false,"voucherType": "STOCK_ADJUSTMENT", "name": {"$concat": ["$name", " StkAdj"]}, "displayName": {"$concat": ["$displayName", " StkAdj"]}}}], 
        None
    )
    .await.unwrap();
}
