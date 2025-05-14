use std::collections::HashMap;

use serde_json::{json, Value};

use super::*;

pub struct Voucher;

pub const VOUCHER_COLLECTION: [&str; 8] = [
    "payments",
    "contras",
    "receipts",
    "journals",
    "purchases",
    "credit_notes",
    "debit_notes",
    "sales",
];

impl Voucher {
    fn voucher_no(v_no: &str, branch_prefix: &str, fy: &str) -> (String, i32, i32) {
        let mut alpha = v_no.split(char::is_numeric).collect::<String>();
        let numeric = v_no.split(char::is_alphabetic).collect::<String>();
        let mut seq = numeric.clone().split(fy).collect::<String>();
        if seq.is_empty() {
            seq = numeric;
        }
        if alpha.is_empty() {
            alpha = branch_prefix.to_string();
        }
        (
            alpha.clone(),
            fy.parse::<i32>().unwrap(),
            seq.parse::<i32>().unwrap(),
        )
    }
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let gst_taxes = HashMap::from([
            ("gstna", 0.0),
            ("gstexempt", 0.0),
            ("gstngs", 0.0),
            ("gst0", 0.0),
            ("gst0p1", 0.1),
            ("gst0p25", 0.25),
            ("gst1", 1.0),
            ("gst1p5", 1.5),
            ("gst3", 3.0),
            ("gst5", 5.0),
            ("gst7p5", 7.5),
            ("gst12", 12.0),
            ("gst18", 18.0),
            ("gst28", 28.0),
        ]);
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "name": 1, "voucherNoPrefix": 1},
                    doc! {},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let financial_years = mongodb
            .collection::<Document>("financial_years")
            .find(doc! {}, None)
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let voucher_types = mongodb
            .collection::<Document>("voucher_types")
            .find(
                doc! {"postgres": {"$exists": true}},
                find_opts(doc! {"_id": 1, "postgres": 1, "voucherType": 1}, doc! {}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(doc! {}, find_opts(doc! {"_id": 1, "postgres": 1}, doc! {}))
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let bank_txns = mongodb
            .collection::<Document>("bank_transactions")
            .find(
                doc! {"bankDate": {"$exists": true}},
                find_opts(doc! {"_id": 1, "bankDate": 1}, doc! {}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        postgres.execute("create or replace function insert_bank_allocation(voucher, jsonb, ac_txn)
    returns bool as
$$
declare
    alt_acc account;
    i       json;
    _sno    smallint := 1;
begin
    for i in select jsonb_array_elements($2)
        loop
            select * into alt_acc from account where id = (i ->> 'alt_account_id')::int;
            insert into bank_txn (id, sno, ac_txn_id, date, inst_date, inst_no, in_favour_of, is_memo, amount,
                                  account_id, account_name, base_account_types, alt_account_id, alt_account_name,
                                  particulars, branch_id, branch_name, voucher_id, voucher_no, base_voucher_type,
                                  txn_type, bank_date)
            values (coalesce((i ->> 'id')::uuid, gen_random_uuid()), _sno, $3.id, $1.date, (i ->> 'inst_date')::date,
                    (i ->> 'inst_no')::text, (i ->> 'in_favour_of')::text, $3.is_memo, (i ->> 'amount')::float,
                    $3.account_id, $3.account_name, $3.base_account_types, alt_acc.id, alt_acc.name,
                    (i ->> 'particulars')::text, $1.branch_id, $1.branch_name, $1.id, $1.voucher_no,
                    $1.base_voucher_type, (i ->> 'txn_type')::text, (i ->> 'bank_date')::date);
            _sno = _sno + 1;
        end loop;
    return true;
end;
$$ language plpgsql;", &[]).await.unwrap();

        postgres.execute("create or replace function insert_bill_allocation(voucher, jsonb, ac_txn)
    returns bool as
$$
declare
    agent_acc account;
    ba        bill_allocation;
    i         json;
    p_id      uuid;
    _sno      smallint := 1;
    rt        text;
begin
    select * into agent_acc from account where id = (select agent_id account where id = $3.account_id);
    for i in select jsonb_array_elements($2)
        loop
            rt = (i ->> 'ref_type');
            if rt = 'NEW' then
                p_id = coalesce((i ->> 'pending')::uuid, gen_random_uuid());
                select * into ba from bill_allocation where pending=p_id and ref_type='NEW';
                if ba is not null then
                    raise exception 'This new ref already exist';
                end if;
            elseif rt = 'ADJ' then
                p_id = (i ->> 'pending')::uuid;
                if p_id is null then raise exception 'pending must be required for adjusted ref'; end if;
                select * into ba from bill_allocation where pending=p_id and ref_type='NEW';
            else
                p_id = null;
            end if;
            insert into bill_allocation (id, sno, ac_txn_id, date, eff_date, is_memo, account_id, account_name, base_account_types,
                                         branch_id, branch_name, amount, pending, ref_type, voucher_id, ref_no,
                                         base_voucher_type, voucher_mode, voucher_no, agent_id, agent_name,
                                         is_approved)
            values (coalesce((i ->> 'id')::uuid, gen_random_uuid()), _sno, $3.id, $1.date,coalesce($1.eff_date, $1.date),
                     $3.is_memo, $3.account_id, $3.account_name, $3.base_account_types,
                     $1.branch_id, $1.branch_name, (i ->> 'amount')::float,
                    p_id, rt, $1.id,
                    (case when rt='ADJ' then coalesce((i ->> 'ref_no')::text, ba.ref_no) else coalesce((i ->> 'ref_no')::text, $3.ref_no) end),
                    (case when rt='ADJ' then ba.base_voucher_type else $1.base_voucher_type end),
                    (case when rt='ADJ' then ba.voucher_mode else $1.mode end),
                    (case when rt='ADJ' then ba.voucher_no else $1.voucher_no end),
                    agent_acc.id,agent_acc.name,$1.require_no_of_approval = $1.approval_state);
            _sno = _sno + 1;
        end loop;
    return true;
end;
$$ language plpgsql;", &[]).await.unwrap();

        postgres
            .execute(
                "create or replace function create_voucher_via_script(json)
    returns bool
as
$$
declare
    v_voucher   voucher;
    first_txn   json := (($1 ->> 'ac_trns')::jsonb)[0];
    _party_name text := (select name
                         from account
                         where id = (first_txn ->> 'account_id')::int);
begin
    insert into voucher (date, branch_id, branch_name, voucher_type_id, branch_gst, party_gst, eff_date, mode, lut, rcm,
                         ref_no, party_id, party_name, credit, debit, description, amount, e_invoice_details,
                         voucher_seq, voucher_prefix, voucher_fy, voucher_no, base_voucher_type, session)
    values (($1 ->> 'date')::date, ($1 ->> 'branch_id')::int, ($1 ->> 'branch_name')::text,
            ($1 ->> 'voucher_type_id')::int, ($1 ->> 'branch_gst')::json, ($1 ->> 'party_gst')::json,
            ($1 ->> 'eff_date')::date, coalesce(($1 ->> 'mode')::text, 'ACCOUNT'), ($1 ->> 'lut')::bool,
            ($1 ->> 'rcm')::bool, ($1 ->> 'ref_no')::text, (first_txn ->> 'account_id')::int, _party_name,
            (first_txn ->> 'credit')::float, (first_txn ->> 'debit')::float, ($1 ->> 'description')::text,
            ($1 ->> 'amount')::float, ($1 ->> 'e_invoice_details')::jsonb, ($1 ->> 'voucher_seq')::int,
            ($1 ->> 'voucher_prefix')::text, ($1 ->> 'voucher_fy')::int, ($1 ->> 'voucher_no')::text,
            ($1 ->> 'base_voucher_type')::text, gen_random_uuid())
    returning * into v_voucher;
    if jsonb_array_length(coalesce(($1 ->> 'ac_trns')::jsonb, '[]'::jsonb)) > 0 then
        perform insert_ac_txn(v_voucher, ($1 ->> 'ac_trns')::jsonb);
    end if;
    return true;
end;
$$ language plpgsql;",
                &[],
            )
            .await
            .unwrap();
        for collection in VOUCHER_COLLECTION {
            println!("start {}...", collection);
            let mut cur = mongodb
            .collection::<Document>(collection)
            .find(
                doc! {"act": false, "date": {"$gte": "2025-01-01"}},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0, "invTrns": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
            while let Some(Ok(d)) = cur.next().await {
                let mut gst_location_type = None;
                let mut party_gst = None;
                let mut branch_gst = None;
                if let Some(br_gst) = d._get_document("branchGst") {
                    if !br_gst.is_empty() {
                        gst_location_type = Some("LOCAL");
                        branch_gst = Some(json!({
                            "reg_type": br_gst.get_str("regType").unwrap(), 
                            "location_id": br_gst.get_str("location").unwrap(),
                            "gst_no": br_gst.get_str("gstNo").unwrap()}));
                    }
                    if let Some(gst) = d._get_document("partyGst") {
                        if !gst.is_empty() {
                            if gst.get_str("location").unwrap_or_default()
                                != br_gst.get_str("location").unwrap()
                            {
                                gst_location_type = Some("INTER_STATE");
                            }
                            let reg_type = if gst.get_str("regType").unwrap() == "CONSUMER" {
                                "UNREGISTERED"
                            } else if gst.get_str("regType").unwrap() == "OVERSEAS" {
                                "IMPORT_EXPORT"
                            } else {
                                gst.get_str("regType").unwrap()
                            };
                            let mut p_gst = json!({
                                "reg_type": reg_type, 
                                "location": gst.get_str("location").ok()});
                            if let Ok(x) = gst.get_str("gstNo") {
                                p_gst["gst_no"] = json!(x);
                            }
                            party_gst = Some(p_gst);
                        }
                    }
                }

                let (branch, branch_name, branch_prefix) = branches
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                            .then_some((
                                x._get_i32("postgres").unwrap(),
                                x.get_str("name").unwrap(),
                                x.get_str("voucherNoPrefix").unwrap(),
                            ))
                    })
                    .unwrap();
                let fy = financial_years
                    .iter()
                    .find(|x| {
                        x.get_string("fStart").unwrap() <= d.get_string("date").unwrap()
                            && x.get_string("fEnd").unwrap() >= d.get_string("date").unwrap()
                    })
                    .unwrap();
                let fy = format!(
                    "{}{}",
                    &fy.get_string("fStart").unwrap()[2..=3],
                    &fy.get_string("fEnd").unwrap()[2..=3]
                );
                let (voucher_type, base_voucher_type) = voucher_types
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap()
                            == d.get_object_id("voucherTypeId").unwrap())
                        .then_some((
                            x._get_i32("postgres").unwrap(),
                            x.get_str("voucherType").unwrap(),
                        ))
                    })
                    .unwrap();
                let voucher_no =
                    Self::voucher_no(&d.get_string("voucherNo").unwrap(), branch_prefix, &fy);
                let mut ac_trns: Vec<Value> = Vec::new();
                let mut voucher_mode = "ACCOUNT";
                let trns = d.get_array_document("acTrns").unwrap_or_default();
                let mut amount = 0.0;
                for trn in trns.clone() {
                    let txn_id = trn.get_object_id("_id").unwrap();
                    if trn.get_str("accountType").unwrap() != "STOCK" {
                        amount += trn._get_f64("debit").unwrap();
                    }
                    let mut ba: Vec<Value> = Vec::new();
                    let mut bk: Vec<Value> = Vec::new();
                    if [
                        "TRADE_PAYABLE",
                        "TRADE_RECEIVABLE",
                        "ACCOUNT_PAYABLE",
                        "ACCOUNT_RECEIVABLE",
                    ]
                    .contains(&trn.get_str("accountType").unwrap())
                    {
                        let mut on_acc_val =
                            trn._get_f64("debit").unwrap() - trn._get_f64("credit").unwrap();
                        let allocs = mongodb
                                .collection::<Document>("bill_allocations")
                                .find(
                                    doc! {"txnId": txn_id},
                                    find_opts(
                                        doc! {"txnId": 1, "amount": 1, "refNo": 1, "pending": 1, "refType": 1, "_id": 0},
                                        doc! {},
                                    ),
                                )
                                .await
                                .unwrap()
                                .try_collect::<Vec<Document>>()
                                .await
                                .unwrap();
                        for alloc in allocs {
                            let oid = alloc.get_object_id("pending").unwrap().to_hex();
                            let pending = format!(
                                "{}-{}-4{}-{}-{}4444444",
                                oid[0..8].to_owned(),
                                oid[8..12].to_owned(),
                                oid[12..15].to_owned(),
                                oid[15..19].to_owned(),
                                oid[19..24].to_owned(),
                            );
                            let amount = alloc._get_f64("amount").unwrap();
                            on_acc_val -= amount;
                            ba.push(json!({
                                "pending": pending,
                                "amount": amount,
                                "ref_type": alloc.get_str("refType").unwrap(),
                                "ref_no": alloc.get_string("refNo").or(d.get_string("refNo")),
                            }));
                        }
                        if round64(on_acc_val, 2) != 0.0 {
                            ba.push(json!({
                                "amount": on_acc_val,
                                "ref_type": "ON_ACC",
                                "ref_no": d.get_string("refNo"),
                            }));
                        }
                    }
                    if ["BANK_ACCOUNT", "BANK_OD_ACCOUNT"]
                        .contains(&trn.get_str("accountType").unwrap())
                    {
                        let sundry_account = &trns.iter().find_map(|x| {
                            ([
                                "TRADE_PAYABLE",
                                "TRADE_RECEIVABLE",
                                "ACCOUNT_PAYABLE",
                                "ACCOUNT_RECEIVABLE",
                            ]
                            .contains(&x.get_str("accountType").unwrap()))
                            .then_some(x.get_object_id("account").unwrap())
                        });
                        let except_bk_account = &trns.iter().find_map(|x| {
                            (!["BANK_ACCOUNT", "BANK_OD_ACCOUNT"]
                                .contains(&x.get_str("accountType").unwrap()))
                            .then_some(x.get_object_id("account").unwrap())
                        });
                        let first_acc = &trns.first().unwrap().get_object_id("account").unwrap();
                        let account =
                            sundry_account.unwrap_or((*except_bk_account).unwrap_or(*first_acc));
                        let account = accounts
                            .iter()
                            .find_map(|x| {
                                (x.get_object_id("_id").unwrap() == account)
                                    .then_some(x._get_i32("postgres").unwrap())
                            })
                            .unwrap();
                        let bank_date = bank_txns.iter().find_map(|x| {
                            (x.get_object_id("_id").unwrap() == txn_id)
                                .then_some(x.get_string("bankDate").unwrap())
                        });
                        bk.push(json!({
                            "amount": trn._get_f64("debit").unwrap() - trn._get_f64("credit").unwrap(),
                            "txn_type": "CASH",
                            "alt_account_id": account,
                            "bank_date": bank_date
                        }));
                    }
                    let account = accounts
                        .iter()
                        .find_map(|x| {
                            (x.get_object_id("_id").unwrap()
                                == trn.get_object_id("account").unwrap())
                            .then_some(x._get_i32("postgres").unwrap())
                        })
                        .unwrap();
                    let mut ac_trn = json!({
                        "account_id": account,
                        "debit": trn._get_f64("debit").unwrap(),
                        "credit": trn._get_f64("credit").unwrap(),
                        "is_default": trn.get_bool("isDefault").ok()
                    });
                    if !ba.is_empty() {
                        ac_trn["bill_allocations"] = json!(ba);
                    }
                    if !bk.is_empty() {
                        ac_trn["bank_allocations"] = json!(bk);
                    }
                    if let (Some(x), true) = (
                        trn.get_string("tax"),
                        (!["GST_RECEIVABLE", "GST_PAYABLE"]
                            .contains(&trn.get_str("accountType").unwrap())),
                    ) {
                        voucher_mode = "GST";
                        let gst_tax = gst_taxes
                            .clone()
                            .into_iter()
                            .find_map(|y| (y.0 == x).then_some(y.1))
                            .unwrap();
                        let taxable =
                            trn._get_f64("debit").unwrap() + trn._get_f64("credit").unwrap();
                        let mut sgst_amount = 0.0;
                        let mut cgst_amount = 0.0;
                        let mut igst_amount = 0.0;
                        if gst_location_type.unwrap_or_default() == "LOCAL" {
                            sgst_amount = round64(taxable * ((gst_tax / 2.0) / 100.00), 2);
                            cgst_amount = round64(taxable * ((gst_tax / 2.0) / 100.00), 2);
                        } else {
                            igst_amount = round64(taxable * (gst_tax / 100.00), 2);
                        }
                        let gst_tax_info = json!({
                            "gst_tax_id": x,
                            "qty": 1,
                            "taxable_amount":taxable,
                            "sgst_amount": sgst_amount,
                            "cgst_amount": cgst_amount,
                            "igst_amount": igst_amount,
                        });
                        ac_trn["gst_info"] = json!(gst_tax_info);
                    }
                    ac_trns.push(ac_trn);
                }
                let data = serde_json::json!({
                   "date": &d.get_string("date").unwrap(),
                   "eff_date": &d.get_string("effDate"),
                    "branch_id":&branch,
                    "voucher_type_id": &voucher_type,
                    "mode": voucher_mode,
                    "ref_no": &d.get_string("refNo"),
                   "description": &d.get_string("description"),
                   "ac_trns": &serde_json::to_value(ac_trns).unwrap(),
                   "amount": &amount,
                    "lut": &d.get_bool("lut").ok(),
                    "rcm": &d.get_bool("rcm").ok(),
                   "branch_name": &branch_name,
                   "base_voucher_type": &base_voucher_type,
                    "voucher_prefix": &voucher_no.0,
                   "voucher_fy": &voucher_no.1,
                   "voucher_seq": &voucher_no.2,
                   "voucher_no": &d.get_string("voucherNo").unwrap(),
                   "branch_gst": &branch_gst,
                   "party_gst": &party_gst,
                });
                let res = postgres
                    .execute(
                        "select * from create_voucher_via_script($1::json)",
                        &[&data],
                    )
                    .await;
                if let Err(x) = res {
                    panic!("ERROR msg{} \n data: {}", x, data);
                }
            }
            println!("end {}...", collection);
        }
        postgres
            .execute("drop function if exists create_voucher_via_script", &[])
            .await
            .unwrap();
    }
}
