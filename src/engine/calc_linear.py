import pandas as pd


class CalcLinear:
    def __init__(self, loadset_date, commissions, exp_id):
        """Generator of rows for two tables - EffectiveSettlement, EirCommissionSettlement

        Args:
            loadset_date (date): date of loading the data
            commissions (dataframe): table Commissions with data about commissions/costs
        """
        self._exp_id = exp_id
        self._loadset_date = loadset_date

        # filter commissions that where already settled
        self._commissions = commissions.loc[
            commissions["CommissionSettlementDate"] > loadset_date, :
        ]

        self._commissions_dates = self._commissions.loc[
            commissions["CommValue"] != 0, "CollectionDate"
        ]

        if self._commissions_dates.empty:
            self._no_commissions = True
        else:
            self._no_commissions = False
            self._date_of_first_commission = self._commissions_dates.iloc[0]

    def calulate_commission_settlement(self, eir_commission_settlement):
        """calculates rows for eir_commission_settlement and returns them as dataframe

        Args:
            events (dataframe): have only events from this loadset date
            eir_commission_settlement (dataframe): table eir_commission_settlement with data from last calculation
        Returns:
            (dataframe): df with rows that are new or None if there where no commissions
        """
        if self._no_commissions:
            # if there are noe commissions in contract then this table wont exist
            return None

        result = pd.DataFrame()

        # get ids of new commissions
        ids = self._commissions.loc[
            (self._commissions["BusinessDate"] == self._loadset_date)
            & (self._commissions["CommValue"] != 0),
            "EirCommissionExternalId",
        ]

        for id in ids:
            # loop to handle all commissions on same date
            row = self.add_new_commission(id)
            eir_commission_settlement = pd.concat(
                [eir_commission_settlement, row], ignore_index=True
            )

            result = pd.concat([result, row], ignore_index=True)

        # list of commissions ID that are to calculate
        commId = self._commissions.loc[
            (self._commissions["CommValue"] != 0)
            & (self._commissions["BusinessDate"] <= self._loadset_date)
            & (self._commissions["CollectionDate"] != self._loadset_date),
            "EirCommissionExternalId",
        ].tolist()

        rows = self.calculate_rows_for_each_commission(
            commId, eir_commission_settlement
        )

        # eir_commission_settlement = pd.concat(
        #    [eir_commission_settlement, rows], ignore_index=True
        # )

        result = pd.concat([result, rows], ignore_index=True)

        return result

    def add_new_commission(self, id):
        """handles event of new commission, creates row for new commission

        Args:
            cashflow_date (date): collection date of commission
            id (str?): id of this new commission

        Returns:
            dataframe: one row with data about this new commission
        """
        # creates new row commission_settelent when there is new commission
        comm_value = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id, "CommValue"
        ]

        comm_type = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id, "CommType"
        ]

        cash_flow_date = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id, "CollectionDate"
        ]

        commission_sign = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id,
            "CommissionSignType",
        ]

        collection_date = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id, "CollectionDate"
        ]

        commission_settlement_date = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id,
            "CommissionSettlementDate",
        ]

        return pd.DataFrame(
            {
                "BusinessDate": self._loadset_date,
                "EirExposureMapId": self._exp_id,
                "EirCommissionExternalId": id,
                "CashFlowDate": cash_flow_date.item(),
                "AmortizationResultsLinear": 0,
                "AmortizationCumulatedLinear": 0,
                "AmortizationResultsEffective": 0,
                "AmortizationCumulatedEffective": 0,
                "UnsettledCommissionBalance": comm_value.item(),
                "CommissionRatio": 0,
                "CommissionMethod": 1,
                "CommValue": comm_value.item(),
                "CommType": comm_type.item(),
                "CommissionSign": commission_sign.item(),
                "CollectionDate": collection_date.item(),
                "CommissionSettlementDate": commission_settlement_date.item(),
            },
            index=[0],
        )

    def calculate_rows_for_each_commission(
        self, comm_id_list, eir_commission_settlement
    ):
        """calculates records for each commission on this cashflow date

        Args:
            comm_id_list (list): list of ids of all commission
            cashflow_date (date): date for calculation
            eir_commission_settlement (dataframe): most actual table!

        Returns:
            dataframe: returns data frame with new rows that where calculated
        """
        # calculates one row for each commission in exp, and returns df with all of them
        results = pd.DataFrame()
        cashflow_date = self._loadset_date

        for id in comm_id_list:
            table_with_one_commission = eir_commission_settlement.loc[
                eir_commission_settlement["EirCommissionExternalId"] == id
            ].reset_index(drop=True)

            row = self.calculate_record(
                id, cashflow_date, table_with_one_commission
            )
            results = pd.concat([results, row])

        return results

    def calculate_record(self, id, cashflow_date, eir_commission_settlement):
        """calculates one record for commission settlement

        Args:
            id (str?): id of commission
            cashflow_date (date): date for calculations
            eir_commission_settlement (dataframe): most recent one!

        Returns:
            dataframe: returns one row of eir_commission_settlement
        """

        last_cashflow_date = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CashFlowDate",
        ]

        commValue = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id,
            "CommValue",
        ]

        settlement_date = pd.to_datetime(
            self._commissions.loc[
                self._commissions["EirCommissionExternalId"] == id,
                "CommissionSettlementDate",
            ].item()
        ).date()

        open_date = self._commissions.loc[
            self._commissions["EirCommissionExternalId"] == id,
            "CollectionDate",
        ].item()

        AmortizationResultsLinear = (
            ((cashflow_date - last_cashflow_date) * commValue)
            / (settlement_date - open_date)
        ).item()

        last_UnsettledCommissionBalance = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "UnsettledCommissionBalance",
        ]

        last_AmortizationCumulatedLinear = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "AmortizationCumulatedLinear",
        ]

        comm_value = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CommValue",
        ]
        comm_type = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CommType",
        ]

        commission_sign = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CommissionSign",
        ]

        return pd.DataFrame(
            {
                "BusinessDate": self._loadset_date,
                "EirExposureMapId": self._exp_id,
                "EirCommissionExternalId": id,
                "CashFlowDate": cashflow_date,
                "AmortizationResultsLinear": AmortizationResultsLinear,
                "AmortizationCumulatedLinear": last_AmortizationCumulatedLinear
                + AmortizationResultsLinear,
                "AmortizationResultsEffective": 0,
                "AmortizationCumulatedEffective": 0,
                "UnsettledCommissionBalance": last_UnsettledCommissionBalance
                - AmortizationResultsLinear,
                "CommissionRatio": 0,
                "CommissionMethod": 1,
                "CommValue": comm_value,
                "CommType": comm_type,
                "CommissionSign": commission_sign,
                "CollectionDate": open_date,
                "CommissionSettlementDate": settlement_date,
            },
            index=[0],
        )
