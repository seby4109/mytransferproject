import pandas as pd

from src import custom_exceptions
from src.utils import error_location


def prepare_data_for_calc(
    id,
    loadset_date,
    exp_info_pack,
    commissions_pack,
    payment_schedule_pack,
    eir_effective_pack,
    commission_settlement_pack,
    effective_settlement_pack,
    is_new_pack,
    is_end_pack,
):

    exp_info = exp_info_pack[
        exp_info_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    commissions = commissions_pack[
        commissions_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    payment_schedule = payment_schedule_pack[
        payment_schedule_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    eir_effective = eir_effective_pack[
        eir_effective_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    commission_settlement = commission_settlement_pack[
        commission_settlement_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    effective_settelment = effective_settlement_pack[
        effective_settlement_pack["EirExposureMapId"] == id
    ].reset_index(drop=True)
    is_new = id in is_new_pack
    is_end = id in is_end_pack

    if loadset_date < exp_info["OpenDate"].iloc[0]:
        raise custom_exceptions.OpenDateIsLaterThanBusinessDate(
            f"{error_location()}"
        )

    set_settlement_date(commissions, exp_info, loadset_date)

    # TODO: confirm values of CommissionMethod
    commissions_for_effective = commissions[
        commissions["CommissionMethodType"] == 2
    ].reset_index(drop=True)
    commissions_for_linear = commissions[
        commissions["CommissionMethodType"] == 1
    ].reset_index(drop=True)

    return (
        exp_info,
        payment_schedule,
        eir_effective,
        commission_settlement,
        effective_settelment,
        commissions_for_effective,
        commissions_for_linear,
        is_new,
        is_end,
    )


def set_settlement_date(commissions, exp_info, loadset_date):
    commId = commissions.loc[
        (commissions["CommValue"] != 0)
        & (commissions["BusinessDate"] <= loadset_date),
        "EirCommissionExternalId",
    ].tolist()

    for id in commId:
        commissions.loc[
            commissions["EirCommissionExternalId"] == id,
            "CommissionSettlementDate",
        ] = get_settlement_date(
            commissions.loc[
                commissions["EirCommissionExternalId"] == id, "CollectionDate"
            ],
            commissions.loc[
                commissions["EirCommissionExternalId"] == id, "SettlementType"
            ],
            exp_info.loc[len(exp_info) - 1, "MaturityDate"],
            id,
        )


def get_settlement_date(collection_date, settlement_type, maturity, id):
    # TODO: set settlement_types to correct values in if's

    if len(settlement_type) > 1:
        raise custom_exceptions.WrongSettlementConfiguration(
            (
                f"Multiple configurations found for given combination of (product type, product subtype, commission type). "
                f"Commission ID = {id}. {error_location()}"
            )
        )
    elif settlement_type.dropna().empty:
        raise custom_exceptions.WrongSettlementConfiguration(
            (
                f"No configuration found for given combination of (product type, product subtype, commission type). "
                f"Commission ID = {id}. {error_location()}"
            )
        )

    settlement_type = settlement_type.item()
    if settlement_type == 1:
        # 1Y
        return collection_date + pd.DateOffset(years=1)
    elif settlement_type == 0:
        # to maturity
        return maturity
    # rest won't be used?
    elif settlement_type == "2Y":
        return collection_date + pd.DateOffset(years=1)
    elif settlement_type == "1M":
        return collection_date + pd.DateOffset(month=1)

    else:
        # Error wrong value of settlement type
        raise custom_exceptions.WrongSettlementConfiguration(
            (
                f"Wrong value of the settlement type! [{settlement_type}] is not a valid settlement type! "
                f"Settlement type should take one of the following values [0, 1, 1M, 2Y]. "
                f"Commission ID = {id}. {error_location()}"
            )
        )
