-- fetch invoice and sales receipt details
SELECT
    "RefId",
    "SaleDate",
    "CustomerName" AS "Customer",
    "State",
    "AgencyID",
    "AgencyName",
    "CustomerPO",
    "RefNumber",
    "SaleTotalAmount",
    "PaidAmount",
    "CreditMemoAmount" AS "Credits/Refunds",
    "CommissionableAmount",
    "DatePaid",
    CASE 
        WHEN "PaidAmount" >= "SaleNetAmount" THEN 'Paid' 
        WHEN "PaidAmount" = 0 THEN 'Unpaid' 
        ELSE 'Partial paid' 
    END AS "paid_status",
    "CommissionPaidDate",
    "CommissionPercent",
    "Notes",
    "Voided",
    "SaleType"
FROM (
    -- invoice details
    SELECT 
        'INVOICE' AS "Source",
        i.id AS "RefId",
        i.txndate::date AS "SaleDate",
        i.customerref__name AS "CustomerName",
        i.billaddr__countrysubdivisioncode AS "State",
        ic.stringvalue AS "CustomerPO",
        i.docnumber AS "RefNumber",
        GREATEST(0, (i.totalamt::numeric - COALESCE(ip."CreditMemoAmount", 0))) AS "SaleNetAmount",
        i.totalamt::numeric AS "SaleTotalAmount",
        COALESCE(ip."PaidAmount", 0) AS "PaidAmount",
        LEAST(COALESCE(ip."CreditMemoAmount", 0), i.totalamt::numeric) AS "CreditMemoAmount",
        GREATEST(0, (i.totalamt::numeric - COALESCE(iline_ship.amount::numeric, 0) - COALESCE(ip."CreditMemoAmount", 0))) AS "CommissionableAmount",
        ip."PaymentDate" AS "DatePaid",
        sta.commission_paid_date::date AS "CommissionPaidDate",
        sta.notes AS "Notes",
        i.privatenote ~~* 'voided%'::text AS "Voided",
        COALESCE(sta.override_commission_percent, ca.commission_percentage) AS "CommissionPercent",
        'INVOICE' AS "SaleType",
        sta.agency_ref AS "AgencyID",
        ca.agency_name AS "AgencyName"
    FROM 
        quickbooks.invoices i
    LEFT JOIN 
        (SELECT 
            i."ref_id",
            SUM(COALESCE(p.amount::numeric, 0)) AS "PaidAmount",
            SUM(CASE WHEN cm.docnumber IS NOT NULL THEN cm.totalamt::numeric ELSE 0 END) AS "CreditMemoAmount"
        FROM 
            quickbooks.invoices i
        LEFT JOIN 
            quickbooks.payments__line__linkedtxn plinetxn ON i.id = plinetxn.txnid AND plinetxn.txntype = 'Invoice'
        LEFT JOIN 
            quickbooks.payments p ON plinetxn._sdc_source_key_id = p.id
        LEFT JOIN 
            quickbooks.payments__line__lineex__any plineex_cm ON plineex_cm._sdc_source_key_id = p.id
                AND plineex_cm.value__name = 'txnReferenceNumber'
                AND EXISTS (SELECT 1 FROM quickbooks.credit_memos cm WHERE cm.docnumber = plineex_cm.value__value)
        LEFT JOIN 
            quickbooks.credit_memos cm ON cm.docnumber = plineex_cm.value__value
        GROUP BY 
            i."ref_id"
        ) ip ON i.id = ip."ref_id"
    LEFT JOIN 
        quickbooks.sale_to_agency sta ON sta.ref_id = i.id AND sta.sale_type = 'INVOICE'
    LEFT JOIN 
        quickbooks.commission_agencies ca ON ca.agency_id = sta.agency_ref AND ca.yr = EXTRACT(YEAR FROM i.txndate::date)
    LEFT JOIN 
        quickbooks.invoices__line iline_ship ON i.id = iline_ship._sdc_source_key_id AND iline_ship.salesitemlinedetail__itemref__value = 'SHIPPING_ITEM_ID' AND iline_ship.detailtype = 'SalesItemLineDetail'
    LEFT JOIN 
        quickbooks.invoices__customfield ic ON ic._sdc_source_key_id = i.id AND ic.name = 'Customer PO'::text
    WHERE 
        i.txndate::date BETWEEN {{daterangepicker1.startFormattedString}}::date AND {{daterangepicker1.endFormattedString}}::date
        AND (sta."AgencyID" IS NOT NULL AND sta."AgencyID" NOT IN (29, 30))
    UNION ALL
    -- Receipt details
    SELECT 
        'SALES RECEIPT' AS "Source",
        sr.id AS "RefId",
        sr.txndate::date AS "SaleDate",
        sr.customerref__name AS "CustomerName",
        sr.shipaddr__countrysubdivisioncode AS "State",
        src.stringvalue AS "CustomerPO",
        sr.docnumber AS "RefNumber",
        sr.totalamt::numeric AS "SaleNetAmount",
        sr.totalamt::numeric AS "SaleTotalAmount",
        COALESCE(sp."PaidAmount", 0) AS "PaidAmount",
        COALESCE(sp."RefundAmount", 0) AS "CreditMemoAmount",
        GREATEST(0, (sr.totalamt::numeric - COALESCE(srline_ship.amount::numeric, 0) - COALESCE(sp."RefundAmount", 0))) AS "CommissionableAmount",
        sr.txndate::date AS "DatePaid",
        sta.commission_paid_date::date AS "CommissionPaidDate",
        sta.notes AS "Notes",
        sr.privatenote ~~* 'voided%'::text AS "Voided",
        COALESCE(sta.override_commission_percent, ca.commission_percentage) AS "CommissionPercent",
        'SALES RECEIPT' AS "SaleType",
        sta.agency_ref AS "AgencyID",
        ca.agency_name AS "AgencyName"
    FROM 
        quickbooks.sales_receipts sr
    LEFT JOIN 
        (SELECT 
            sr.id AS "SalesReceiptId",
            sr.totalamt::numeric AS "SalesReceiptTotalAmount",
            COALESCE(r.refund_amount::numeric, 0) AS "RefundAmount",
            SUM(COALESCE(srline.amount::numeric, 0)) AS "PaymentReceived"
        FROM 
            quickbooks.sales_receipts sr
        LEFT JOIN 
            (SELECT split_part(rr.docnumber, '-'::text, 1) AS paymentrefnum,
                    SUM(COALESCE(rr.totalamt::numeric, 0)) AS refund_amount
             FROM quickbooks.refund_receipts rr
             GROUP BY 1) r ON r.paymentrefnum = sr.paymentrefnum
        LEFT JOIN 
            quickbooks.sales_receipts__line srline ON sr.id = srline._sdc_source_key_id
        GROUP BY 
            sr.id, r.refund_amount
        ) sp ON sr.id = sp."SalesReceiptId"
    LEFT JOIN 
        quickbooks.sale_to_agency sta ON sta.ref_id = sr.id AND sta.sale_type = 'SALES RECEIPT'
    LEFT JOIN 
        quickbooks.commission_agencies ca ON ca.agency_id = sta.agency_ref AND ca.yr = EXTRACT(YEAR FROM sr.txndate::date)
    LEFT JOIN 
        quickbooks.sales_receipts__line srline_ship ON sr.id = srline_ship._sdc_source_key_id AND srline_ship.salesitemlinedetail__itemref__value = 'SHIPPING_ITEM_ID' AND srline_ship.detailtype = 'SalesItemLineDetail'
    LEFT JOIN 
        quickbooks.sales_receipts__customfield src ON src._sdc_source_key_id = sr.id AND src.name = 'Customer PO'::text
    WHERE 
        sr.txndate::date BETWEEN {{daterangepicker1.startFormattedString}}::date AND {{daterangepicker1.endFormattedString}}::date
        AND (sta."AgencyID" IS NOT NULL AND sta."AgencyID" NOT IN (29, 30))
) AS "InvoicesUnionSalesReceipts"
ORDER BY 
    "RefId" DESC;

