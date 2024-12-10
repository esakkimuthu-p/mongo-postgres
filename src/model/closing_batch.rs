use mongodb::{bson::oid::ObjectId, options::AggregateOptions, IndexModel};

use super::*;

pub struct InventoryBranchBatch;

impl InventoryBranchBatch {
    pub async fn create(mongodb: &Database) {
        let _ = mongodb
            .collection::<Document>("batches")
            .aggregate(vec![
                doc!{ "$match": { "$expr" : { "$ne" : [ "$inward", "$outward" ] }}},
                doc!{
                    "$project": {
                        "inventory": 1, "branch": 1, "sRate": {"$round": ["$sRate", 2]}, "mrp": {"$round": ["$mrp", 2]},
                        "rate": {"$ifNull": [{"$divide": ["$pRate","$unitConv"]}, {"$ifNull": ["$avgNlc", 0.0]}]},
                        "batchNo": 1, "expiry": 1,"avgNlc": 1,
                        "qty": { "$round": [{ "$subtract": ["$inward", "$outward"] }, 4] },
                        "unitConv":{"$ifNull": ["$unitConv", 1.0]},
                        "barcode": {"$toString": "$barcode"},
                    }
                },
                doc!{ "$out": "closing_batches"}
            ],
            AggregateOptions::builder().allow_disk_use(true).build(),
            )
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
        let x = mongodb
            .collection::<Document>("closing_batches")
            .aggregate(
                vec![
                    doc! { "$match": { "qty": { "$lt": 0 } } },
                    doc! {
                        "$group": {
                            "_id":null,
                            "ids": {"$addToSet": "$inventory"},
                        }
                    },
                ],
                AggregateOptions::builder().allow_disk_use(true).build(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let y = x.first().map(|z| {
            z.get_array("ids").map(|p| {
                p.iter()
                    .filter_map(|q| q.as_object_id())
                    .collect::<Vec<ObjectId>>()
            })
        });
        if let Some(Ok(a)) = y {
            mongodb
                .collection::<Document>("inventories")
                .update_many(
                    doc! {"_id": {"$in": a}},
                    doc! {"$set": {"allowNegativeStock": true}},
                    None,
                )
                .await
                .unwrap();
        }
    }
    pub async fn opening(mongodb: &Database, postgres: &PostgresClient) {
        mongodb
            .collection::<Document>("closing_batches")
            .aggregate(
                vec![
                    doc! {"$match": {"postgres": {"$exists": true}}},
                    doc! {
                        "$group": {
                            "_id": {"branch": "$branch", "postgres": "$postgres"},
                            "inv_items": {"$push": {
                               "sno": 1,
                               "qty": "$qty",
                               "nlc": {"$ifNull": ["$avgNlc", {"$ifNull": ["$rate", 0.0]}]},
                               "cost": {"$ifNull": ["$avgNlc", {"$ifNull": ["$rate", 0.0]}]},
                               "unit_id": "$postgres_unit",
                               "unit_conv": 1,
                               "is_retail_qty": {"$literal": true},
                               "rate": {"$round": ["$rate", 2]},
                               "batch_no": "$batchNo",
                               "mrp": {"$round": ["$mrp", 2]},
                               "s_rate": {"$round": ["$sRate", 2]},
                               "expiry": "$expiry",
                               "barcode": "$barcode",
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
                AggregateOptions::builder().allow_disk_use(true).build(),
            )
            .await
            .unwrap();
        postgres
            .execute("alter table inventory_opening add barcode text", &[])
            .await
            .ok();
        postgres.execute("create or replace function set_inventory_opening(input json)
    returns bool as
$$
declare
    _items         inventory_opening[] := (select array_agg(x)
                                           from jsonb_populate_recordset(
                                                        null::inventory_opening,
                                                        ($1 ->> 'inv_items')::jsonb) as x);
    _inventory     inventory           := (select inventory
                                           from inventory
                                           where id = ($1 ->> 'inventory_id')::int);
    _units         unit[]              := (select array_agg(a)
                                           from unit a
                                           where a.id in
                                                 (select x.unit_id
                                                  from unnest(_items) x));
    _division      division            := (select division
                                           from division
                                           where id = _inventory.division_id);
    _warehouse     warehouse           := (select warehouse
                                           from warehouse
                                           where id = ($1 ->> 'warehouse_id')::int);
    _branch        branch              := (select branch
                                           from branch
                                           where id = ($1 ->> 'branch_id')::int);
    _book_begin    date                := (select book_begin - 1
                                           from organization
                                           limit 1);
    _item          inventory_opening;
    _batch         batch;
    retail         int;
    _cost          float;
    _landing_cost  float;
    _nlc           float;
    _unit_name     text;
begin
    foreach _item in array coalesce(_items, array []::inventory_opening[])
        loop
            if _item.is_retail_qty then
                retail = 1;
                _cost = round((_item.cost * _inventory.retail_qty)::numeric, 4)::float;
                _nlc = round((_item.nlc * _inventory.retail_qty)::numeric, 4)::float;
                _landing_cost = round((_item.landing_cost * _inventory.retail_qty)::numeric, 4)::float;
            else
                retail = _inventory.retail_qty;
                _cost = _item.cost;
                _nlc = _item.nlc;
                _landing_cost = _item.landing_cost;
            end if;
            insert into inventory_opening (id, sno, inventory_id, branch_id, warehouse_id, unit_id, unit_conv, qty, nlc,
                                           cost, rate, is_retail_qty, landing_cost, mrp, s_rate, batch_no, expiry,
                                           asset_amount, barcode)
            values (coalesce(_item.id, gen_random_uuid()), _item.sno, _inventory.id, _branch.id, _warehouse.id,
                    _item.unit_id, _item.unit_conv, _item.qty, _item.nlc, _item.cost, _item.rate, _item.is_retail_qty,
                    _item.landing_cost, _item.mrp, _item.s_rate, _item.batch_no, _item.expiry, _item.asset_amount, _item.barcode)
            returning * into _item;
            select u.name into _unit_name from unnest(_units) u where u.id = _item.unit_id;
            insert into batch (txn_id, sno, inventory_id, reorder_inventory_id, inventory_name, inventory_hsn,
                               branch_id, branch_name, warehouse_id, warehouse_name, division_id, division_name,
                               entry_type, batch_no, expiry, entry_date, mrp, s_rate, opening_p_rate, landing_cost, nlc,
                               cost, unit_id, unit_name, unit_conv, manufacturer_id, manufacturer_name, retail_qty,
                               label_qty, is_retail_qty, inward, barcode)
            values (_item.id, _item.sno, _item.inventory_id, coalesce(_inventory.reorder_inventory_id, _inventory.id),
                    _inventory.name, _inventory.hsn_code, _item.branch_id, _branch.name, _item.warehouse_id,
                    _warehouse.name, _division.id, _division.name, 'OPENING', _item.batch_no, _item.expiry, _book_begin,
                    _item.mrp, _item.s_rate, _item.rate, _landing_cost, _nlc, _cost, _item.unit_id, _unit_name,
                    _item.unit_conv, _inventory.manufacturer_id, _inventory.manufacturer_name, _inventory.retail_qty,
                    _item.qty * _item.unit_conv, _item.is_retail_qty, _item.qty * _item.unit_conv * retail, _item.barcode)
            returning * into _batch;

            insert into inv_txn(id, date, branch_id, division_id, division_name, branch_name, batch_id, inventory_id,
                                reorder_inventory_id, inventory_name, inventory_hsn, manufacturer_id, manufacturer_name,
                                inward, asset_amount, warehouse_id, warehouse_name, is_opening)
            values (_item.id, _book_begin, _branch.id, _division.id, _division.name, _branch.name, _batch.id,
                    _inventory.id, coalesce(_inventory.reorder_inventory_id, _inventory.id), _inventory.name,
                    _inventory.hsn_code, _inventory.manufacturer_id, _inventory.manufacturer_name,
                    _item.qty * _item.unit_conv * retail, _item.asset_amount, _batch.warehouse_id,
                    _batch.warehouse_name, true);
        end loop;
    return true;
end;
$$ language plpgsql security definer;", &[]).await.unwrap();
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
        postgres
            .execute("alter table inventory_opening drop barcode", &[])
            .await
            .ok();
    }
}
