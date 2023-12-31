merge into PUBLIC.TRANSACTIONLINES
using STAGING.TRANSACTIONLINES_RAW
    on (
        TRANSACTIONLINES.TRANSACTIONDATE = TRANSACTIONLINES_RAW.INVOICE_DATE
        and TRANSACTIONLINES.STOREIDENTIFIER = TRANSACTIONLINES_RAW.STORE_CODE
        and TRANSACTIONLINES.LANENUMBER = TRANSACTIONLINES_RAW.TILL_NO
        and TRANSACTIONLINES.RECEIPTNUMBER = TRANSACTIONLINES_RAW.INVOICE_NO
        and TRANSACTIONLINES.SECONDARYPRODUCTIDENTIFIER = TRANSACTIONLINES_RAW.PRODUCT_CODE
        and TRANSACTIONLINES.QUANTITYPURCHASED = TRANSACTIONLINES_RAW.SALE_TOT_QTY
        and TRANSACTIONLINES.PURCHASEPRICE = TRANSACTIONLINES_RAW.SALE_NET_VAL
    )
when not matched then
    insert (
        TRANSACTIONDATE,
        STOREIDENTIFIER,
        LANENUMBER,
        RECEIPTNUMBER,
        SECONDARYPRODUCTIDENTIFIER,
        QUANTITYPURCHASED,
        PURCHASEPRICE
    )
    values (
        TRANSACTIONLINES_RAW.INVOICE_DATE,
        TRANSACTIONLINES_RAW.STORE_CODE,
        TRANSACTIONLINES_RAW.TILL_NO,
        TRANSACTIONLINES_RAW.INVOICE_NO,
        TRANSACTIONLINES_RAW.PRODUCT_CODE,
        TRANSACTIONLINES_RAW.SALE_TOT_QTY,
        TRANSACTIONLINES_RAW.SALE_NET_VAL
    );
