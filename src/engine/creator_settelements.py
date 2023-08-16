import pandas as pd


class CreatorSettelments:
    def __init__(self, loadset_date, commissions, exp_id, eir_effective):
        """Generator of rows for two tables - EffectiveSettlement, EirCommissionSettlement

        Args:
            loadset_date (date): date of loading the data
            commissions (dataframe): table Commissions with data about commissions/costs
        """
        self._eir_effective = eir_effective
        self._exp_id = exp_id
        self._loadset_date = loadset_date
        self._commissions_dates = commissions.loc[
            commissions["CommValue"] != 0, "CollectionDate"
        ]
        self._commissions = commissions

        if self._commissions_dates.empty:
            self._no_commissions = True
        else:
            self._no_commissions = False
            self._date_of_first_commission = self._commissions_dates.iloc[0]

    def calulate_commission_settlement(self, events, eir_commission_settlement):
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

        # list of commissions ID that are to calculate (without those that are added in this loadset date)
        commId = self._commissions.loc[
            (self._commissions["CommValue"] != 0)
            & (self._commissions["BusinessDate"] < self._loadset_date),
            "EirCommissionExternalId",
        ].tolist()

        result = pd.DataFrame()

        for i in range(len(events)):
            date = events.loc[i, "date"]

            if len(eir_commission_settlement) > 0:
                rows = self.calculate_rows_for_each_commission(
                    commId, date, eir_commission_settlement
                )

                eir_commission_settlement = pd.concat(
                    [eir_commission_settlement, rows], ignore_index=True
                )
                result = pd.concat([result, rows], ignore_index=True)

            if "commission" in events.loc[i, "event_type"]:
                # there is commission in this date
                # id of this new commission
                ids = self._commissions.loc[
                    self._commissions["CollectionDate"] == date,
                    "EirCommissionExternalId",
                ]

                for id in ids:
                    # loop to handle all commissions on same date
                    row = self.add_new_commission(date, id)
                    # add to list of id for calculations
                    commId.append(id)

                    eir_commission_settlement = pd.concat(
                        [eir_commission_settlement, row], ignore_index=True
                    )
                    result = pd.concat([result, row], ignore_index=True)

        return result

    def add_new_commission(self, cashflow_date, id):
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
                "CashFlowDate": cashflow_date,
                "AmortizationResultsLinear": 0,
                "AmortizationCumulatedLinear": 0,
                "AmortizationResultsEffective": 0,
                "AmortizationCumulatedEffective": 0,
                "UnsettledCommissionBalance": comm_value.item(),
                "CommissionRatio": 0,
                "CommissionMethod": 2,
                "CommValue": comm_value.item(),
                "CommType": comm_type.item(),
                "CommissionSign": commission_sign.item(),
                "CollectionDate": collection_date.item(),
                "CommissionSettlementDate": commission_settlement_date.item(),
            },
            index=[0],
        )

    def calculate_rows_for_each_commission(
        self, comm_id_list, cashflow_date, eir_commission_settlement
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
        eir_effective = self._eir_effective.loc[
            self._eir_effective["CashFlowDate"] <= cashflow_date
        ]

        last_UnsettledCommissionBalance = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "UnsettledCommissionBalance",
        ]

        CommissionRatio = (
            last_UnsettledCommissionBalance
            / eir_effective["UnsettledCommissionBalanceEffective"].iloc[
                len(eir_effective) - 2
            ]
        )

        AmortizationResultsEffective = (
            eir_effective["AmortizationResultsEffective"].iloc[
                len(eir_effective) - 1
            ]
            * CommissionRatio
        )
        UnsettledCommissionBalance = (
            last_UnsettledCommissionBalance - AmortizationResultsEffective
        )

        last_AmortizationCumulatedEffective = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "AmortizationCumulatedEffective",
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

        collection_date = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CollectionDate",
        ]

        commission_settlement_date = eir_commission_settlement.loc[
            len(eir_commission_settlement) - 1,
            "CommissionSettlementDate",
        ]

        return pd.DataFrame(
            {
                "BusinessDate": self._loadset_date,
                "EirExposureMapId": self._exp_id,
                "EirCommissionExternalId": id,
                "CashFlowDate": cashflow_date,
                "AmortizationResultsLinear": 0,
                "AmortizationCumulatedLinear": 0,
                "AmortizationResultsEffective": AmortizationResultsEffective,
                "AmortizationCumulatedEffective": last_AmortizationCumulatedEffective
                + AmortizationResultsEffective,
                "UnsettledCommissionBalance": UnsettledCommissionBalance,
                "CommissionRatio": CommissionRatio,
                "CommissionMethod": 2,
                "CommValue": comm_value,
                "CommType": comm_type,
                "CommissionSign": commission_sign,
                "CollectionDate": collection_date,
                "CommissionSettlementDate": commission_settlement_date,
            },
            index=[0],
        )

    def generate_rows_EffectiveSettelment(self, effective_settelment):

        """generates all rows for effective settlement

        Returns:
            dataframe: returns n rows(depending on number of cashflows in eir_effective for loadset date), for table Effective settelemnt
        """
        # TODO: check if it works when there is only one event for this loadset date
        if self._no_commissions:
            rows = None

        else:
            eir_from_loadset_after_commission = self._eir_effective.loc[
                (self._eir_effective["BusinessDate"] == self._loadset_date)
                & (
                    self._eir_effective["CashFlowDate"]
                    >= self._date_of_first_commission
                )
            ]

            if len(eir_from_loadset_after_commission) == 0:
                rows = pd.DataFrame(
                    {
                        "BusinessDate": self._loadset_date,
                        "EirExposureMapId": eir_from_loadset_after_commission[
                            "EirExposureMapId"
                        ],
                        "CashFlowDate": eir_from_loadset_after_commission[
                            "CashFlowDate"
                        ],
                        "NominalInterestAccrual": eir_from_loadset_after_commission[
                            "RepaymentAccruedInterest"
                        ],
                        "EffectiveInterestAccrual": eir_from_loadset_after_commission[
                            "EffectiveInterest"
                        ],
                        "AmortizationResultsEffective": eir_from_loadset_after_commission[
                            "AmortizationResultsEffective"
                        ],
                        "UnsettledCommissionBalanceEffective": eir_from_loadset_after_commission[
                            "UnsettledCommissionBalanceEffective"
                        ],
                    },
                    index=[0],
                )
            else:
                rows = pd.DataFrame(
                    {
                        "BusinessDate": self._loadset_date,
                        "EirExposureMapId": eir_from_loadset_after_commission[
                            "EirExposureMapId"
                        ],
                        "CashFlowDate": eir_from_loadset_after_commission[
                            "CashFlowDate"
                        ],
                        "NominalInterestAccrual": eir_from_loadset_after_commission[
                            "RepaymentAccruedInterest"
                        ],
                        "EffectiveInterestAccrual": eir_from_loadset_after_commission[
                            "EffectiveInterest"
                        ],
                        "AmortizationResultsEffective": eir_from_loadset_after_commission[
                            "AmortizationResultsEffective"
                        ],
                        "UnsettledCommissionBalanceEffective": eir_from_loadset_after_commission[
                            "UnsettledCommissionBalanceEffective"
                        ],
                    }
                )

            effective_settelment_len = len(effective_settelment)
            if effective_settelment_len > 0:
                last_amortization_cum = effective_settelment.loc[
                    effective_settelment_len - 1,
                    "AmortizationCumulatedEffective",
                ]
            else:
                last_amortization_cum = 0

            rows["AmortizationCumulatedEffective"] = (
                rows["AmortizationResultsEffective"].cumsum()
                + last_amortization_cum
            )
        return rows
