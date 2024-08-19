use mongodb::{options::AggregateOptions, IndexModel};

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
                        "unitConv":1,
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
        postgres.execute("create or replace function set_inventory_opening(input json)
    returns bool as
$$
declare
    _item          inventory_opening;
    _items         inventory_opening[] := (select array_agg(x)
                                           from jsonb_populate_recordset(
                                                        null::inventory_opening,
                                                        ($1 ->> 'inv_items')::jsonb) as x);
    _inventory     inventory           := (select inventory
                                           from inventory
                                           where id = ($1 ->> 'inventory_id')::int);
    _batch         batch;
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
    _removed_items uuid[];
    retail         int;
    _cost          float;
    _landing_cost  float;
    _nlc           float;
    asset_amt      float;
begin

    select array_agg(x.id)
    into _removed_items
    from ((select id
           from inventory_opening a
           where a.inventory_id = _inventory.id
             and a.branch_id = _branch.id
             and a.warehouse_id = _warehouse.id)
          except
          (select id
           from unnest(_items))) as x;
    delete from inv_txn where id = any (_removed_items);
    delete from inventory_opening where id = any (_removed_items);

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
                                           asset_amount)
            values (coalesce(_item.id, gen_random_uuid()), _item.sno, _inventory.id, _branch.id, _warehouse.id,
                    _item.unit_id, _item.unit_conv, _item.qty, _item.nlc, _item.cost, _item.rate, _item.is_retail_qty,
                    _item.landing_cost, _item.mrp, _item.s_rate, _item.batch_no, _item.expiry, _item.asset_amount)
            on conflict (id) do update
                set unit_id       = excluded.unit_id,
                    unit_conv     = excluded.unit_conv,
                    sno           = excluded.sno,
                    qty           = excluded.qty,
                    is_retail_qty = excluded.is_retail_qty,
                    rate          = excluded.rate,
                    landing_cost  = excluded.landing_cost,
                    nlc           = excluded.nlc,
                    cost          = excluded.cost,
                    mrp           = excluded.mrp,
                    expiry        = excluded.expiry,
                    s_rate        = excluded.s_rate,
                    batch_no      = excluded.batch_no,
                    asset_amount  = excluded.asset_amount
            returning * into _item;

            insert into batch (txn_id, sno, inventory_id, reorder_inventory_id, inventory_name, inventory_hsn,
                               branch_id, branch_name, warehouse_id, warehouse_name, division_id, division_name,
                               entry_type, batch_no, expiry, entry_date, mrp, s_rate, opening_p_rate, landing_cost, nlc,
                               cost, unit_id, unit_conv, manufacturer_id, manufacturer_name, retail_qty, label_qty,
                               is_retail_qty)
            values (_item.id, _item.sno, _item.inventory_id, coalesce(_inventory.reorder_inventory_id, _inventory.id),
                    _inventory.name, _inventory.hsn_code, _item.branch_id, _branch.name, _item.warehouse_id,
                    _warehouse.name, _division.id, _division.name, 'OPENING', _item.batch_no, _item.expiry, _book_begin,
                    _item.mrp, _item.s_rate, _item.rate, _landing_cost, _nlc, _cost, _item.unit_id, _item.unit_conv,
                    _inventory.manufacturer_id, _inventory.manufacturer_name, _inventory.retail_qty,
                    _item.qty * _item.unit_conv, _item.is_retail_qty)
            on conflict (txn_id) do update
                set inventory_name    = excluded.inventory_name,
                    inventory_hsn     = excluded.inventory_hsn,
                    branch_name       = excluded.branch_name,
                    division_name     = excluded.division_name,
                    warehouse_name    = excluded.warehouse_name,
                    sno               = excluded.sno,
                    batch_no          = excluded.batch_no,
                    expiry            = excluded.expiry,
                    entry_date        = excluded.entry_date,
                    label_qty         = excluded.label_qty,
                    is_retail_qty     = excluded.is_retail_qty,
                    mrp               = excluded.mrp,
                    opening_p_rate    = excluded.opening_p_rate,
                    s_rate            = excluded.s_rate,
                    nlc               = excluded.nlc,
                    cost              = excluded.cost,
                    landing_cost      = excluded.landing_cost,
                    unit_conv         = excluded.unit_conv,
                    manufacturer_id   = excluded.manufacturer_id,
                    manufacturer_name = excluded.manufacturer_name
            returning * into _batch;

            insert into inv_txn(id, date, branch_id, division_id, division_name, branch_name, batch_id, inventory_id,
                                reorder_inventory_id, inventory_name, inventory_hsn, manufacturer_id, manufacturer_name,
                                inward, asset_amount, warehouse_id, warehouse_name, is_opening)
            values (_item.id, _book_begin, _branch.id, _division.id, _division.name, _branch.name, _batch.id,
                    _inventory.id, coalesce(_inventory.reorder_inventory_id, _inventory.id), _inventory.name,
                    _inventory.hsn_code, _inventory.manufacturer_id, _inventory.manufacturer_name,
                    _item.qty * _item.unit_conv * retail, _item.asset_amount, _batch.warehouse_id,
                    _batch.warehouse_name, true)
            on conflict (id) do update
                set inventory_name    = excluded.inventory_name,
                    inventory_hsn     = excluded.inventory_hsn,
                    branch_name       = excluded.branch_name,
                    division_name     = excluded.division_name,
                    warehouse_name    = excluded.warehouse_name,
                    inward            = excluded.inward,
                    asset_amount      = excluded.asset_amount,
                    manufacturer_id   = excluded.manufacturer_id,
                    manufacturer_name = excluded.manufacturer_name;
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
        postgres.execute("create or replace function set_inventory_opening(input json)
    returns bool as
$$
declare
    _item          inventory_opening;
    _items         inventory_opening[] := (select array_agg(x)
                                           from jsonb_populate_recordset(
                                                        null::inventory_opening,
                                                        ($1 ->> 'inv_items')::jsonb) as x);
    _inventory     inventory           := (select inventory
                                           from inventory
                                           where id = ($1 ->> 'inventory_id')::int);
    _batch         batch;
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
    _removed_items uuid[];
    retail         int;
    _cost          float;
    _landing_cost  float;
    _nlc           float;
    asset_amt      float;
begin

    select array_agg(x.id)
    into _removed_items
    from ((select id
           from inventory_opening a
           where a.inventory_id = _inventory.id
             and a.branch_id = _branch.id
             and a.warehouse_id = _warehouse.id)
          except
          (select id
           from unnest(_items))) as x;
    delete from inv_txn where id = any (_removed_items);
    delete from inventory_opening where id = any (_removed_items);

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
                                           asset_amount)
            values (coalesce(_item.id, gen_random_uuid()), _item.sno, _inventory.id, _branch.id, _warehouse.id,
                    _item.unit_id, _item.unit_conv, _item.qty, _item.nlc, _item.cost, _item.rate, _item.is_retail_qty,
                    _item.landing_cost, _item.mrp, _item.s_rate, _item.batch_no, _item.expiry, _item.asset_amount)
            on conflict (id) do update
                set unit_id       = excluded.unit_id,
                    unit_conv     = excluded.unit_conv,
                    sno           = excluded.sno,
                    qty           = excluded.qty,
                    is_retail_qty = excluded.is_retail_qty,
                    rate          = excluded.rate,
                    landing_cost  = excluded.landing_cost,
                    nlc           = excluded.nlc,
                    cost          = excluded.cost,
                    mrp           = excluded.mrp,
                    expiry        = excluded.expiry,
                    s_rate        = excluded.s_rate,
                    batch_no      = excluded.batch_no,
                    asset_amount  = excluded.asset_amount
            returning * into _item;

            insert into batch (txn_id, sno, inventory_id, reorder_inventory_id, inventory_name, inventory_hsn,
                               branch_id, branch_name, warehouse_id, warehouse_name, division_id, division_name,
                               entry_type, batch_no, expiry, entry_date, mrp, s_rate, opening_p_rate, landing_cost, nlc,
                               cost, unit_id, unit_conv, manufacturer_id, manufacturer_name, retail_qty, label_qty,
                               is_retail_qty)
            values (_item.id, _item.sno, _item.inventory_id, coalesce(_inventory.reorder_inventory_id, _inventory.id),
                    _inventory.name, _inventory.hsn_code, _item.branch_id, _branch.name, _item.warehouse_id,
                    _warehouse.name, _division.id, _division.name, 'OPENING', _item.batch_no, _item.expiry, _book_begin,
                    _item.mrp, _item.s_rate, _item.rate, _landing_cost, _nlc, _cost, _item.unit_id, _item.unit_conv,
                    _inventory.manufacturer_id, _inventory.manufacturer_name, _inventory.retail_qty,
                    _item.qty * _item.unit_conv, _item.is_retail_qty)
            on conflict (txn_id) do update
                set inventory_name    = excluded.inventory_name,
                    inventory_hsn     = excluded.inventory_hsn,
                    branch_name       = excluded.branch_name,
                    division_name     = excluded.division_name,
                    warehouse_name    = excluded.warehouse_name,
                    sno               = excluded.sno,
                    batch_no          = excluded.batch_no,
                    expiry            = excluded.expiry,
                    entry_date        = excluded.entry_date,
                    label_qty         = excluded.label_qty,
                    is_retail_qty     = excluded.is_retail_qty,
                    mrp               = excluded.mrp,
                    opening_p_rate    = excluded.opening_p_rate,
                    s_rate            = excluded.s_rate,
                    nlc               = excluded.nlc,
                    cost              = excluded.cost,
                    landing_cost      = excluded.landing_cost,
                    unit_conv         = excluded.unit_conv,
                    manufacturer_id   = excluded.manufacturer_id,
                    manufacturer_name = excluded.manufacturer_name
            returning * into _batch;

            insert into inv_txn(id, date, branch_id, division_id, division_name, branch_name, batch_id, inventory_id,
                                reorder_inventory_id, inventory_name, inventory_hsn, manufacturer_id, manufacturer_name,
                                inward, asset_amount, warehouse_id, warehouse_name, is_opening)
            values (_item.id, _book_begin, _branch.id, _division.id, _division.name, _branch.name, _batch.id,
                    _inventory.id, coalesce(_inventory.reorder_inventory_id, _inventory.id), _inventory.name,
                    _inventory.hsn_code, _inventory.manufacturer_id, _inventory.manufacturer_name,
                    _item.qty * _item.unit_conv * retail, _item.asset_amount, _batch.warehouse_id,
                    _batch.warehouse_name, true)
            on conflict (id) do update
                set inventory_name    = excluded.inventory_name,
                    inventory_hsn     = excluded.inventory_hsn,
                    branch_name       = excluded.branch_name,
                    division_name     = excluded.division_name,
                    warehouse_name    = excluded.warehouse_name,
                    inward            = excluded.inward,
                    asset_amount      = excluded.asset_amount,
                    manufacturer_id   = excluded.manufacturer_id,
                    manufacturer_name = excluded.manufacturer_name;
        end loop;

    select coalesce(round(sum(asset_amount)::numeric, 2)::float, 0)
    into asset_amt
    from inv_txn
    where inv_txn.branch_id = _branch.id
      and is_opening = true;

    update ac_txn
    set debit = asset_amt
    where branch_id = _branch.id
      and account_id = 16
      and is_opening = true;

    if not FOUND then
        insert into ac_txn(id, sno, date, account_id, credit, debit, account_name, base_account_types, branch_id,
                           branch_name, is_opening)
        values (gen_random_uuid(), 1, _book_begin, 16, 0.0, asset_amt, 'Inventory Asset', array ['STOCK'], _branch.id,
                _branch.name, true);
    end if;
    return true;
end;
$$ language plpgsql security definer;", &[]).await.unwrap();
    }
}
