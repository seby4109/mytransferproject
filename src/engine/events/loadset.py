import numpy as np
import pandas as pd

from src.engine import eir_cash_flow_schedule
from src.engine.events.event import Event


class Loadset(Event):
    def __init__(
        self,
        parameters_const,
        cash_flow_date,
        payment_schedule_for_init,
        payment_schedule_for_loadset,
        nominal_interest_for_init,
        event_list,
    ):

        super().__init__(
            parameters_const,
            cash_flow_date,
            payment_schedule_for_init,
            nominal_interest_for_init,
        )
        self._event_list = event_list
        self._calculate_eir = False
        self._pre_payment = True
        self._payment_schedule_for_loadset = payment_schedule_for_loadset

    def calculate(self, eir_effective):
        """
        Calculates values for new row of eir_effective

        Args:
            eir_effective (dataframe): most recent table of eir_effective
        """
        self._eir_effective = eir_effective
        last_index_eir_effective = len(self._eir_effective) - 1

        # = principal_repayment from payment shedule on date of cash flow
        # (dziala z zalozeniem, ze tylko jeden payment schedule jest
        # w _eir_payment_schedule)
        self._repayment_capital = self._payment_schedule[
            self._payment_schedule["Date"].to_numpy().astype("datetime64[D]")
            == self._cash_flow_date
        ]["Capital"]

        # check if _principal_repayment_amount_ccy is empty (means that there
        # was no repayment and it's value should be 0)
        if self._repayment_capital.empty:
            self._repayment_capital = 0

        else:
            self._repayment_capital = self._repayment_capital.item()

        self._notional_due = self.calculate_notional_due(
            self._eir_effective.loc[last_index_eir_effective, "NotionalDue"],
            self._repayment_capital,
        )

        pre_payment = (
            self._notional_due
            - self._exp_info.loc[len(self._exp_info) - 1, "Notional"]
        )

        if pre_payment > 0:
            # there was prepayment
            self._repayment_capital += pre_payment
            self._notional_due = self.calculate_notional_due(
                self._eir_effective.loc[
                    last_index_eir_effective, "NotionalDue"
                ],
                self._repayment_capital,
            )
            self._pre_payment = True
            self._calculate_eir = True

        # = interest_repayment from payment shedule on date of cash flow
        # (dziala z zalozeniem, ze tylko jeden payment schedule jest w
        # _eir_payment_schedule)
        self._repayment_interest = self._payment_schedule[
            self._payment_schedule["Date"].to_numpy().astype("datetime64[D]")
            == self._cash_flow_date
        ]["Interest"]

        # if there are no repayment value is set to 0
        if self._repayment_interest.empty:
            self._repayment_interest = 0

        else:
            self._repayment_interest = self._repayment_interest.item()

        self._repayment_accrued_interest = self.calculate_interest(
            self._cash_flow_date,
            self._eir_effective.loc[last_index_eir_effective, "CashFlowDate"],
            self._eir_effective.loc[last_index_eir_effective, "NotionalDue"],
            self._nir,
        )

        # = value of commision for this cash flow date from _eir_commissions
        self._comm_value = self._commissions.loc[
            self._commissions["CollectionDate"] == self._cash_flow_date,
            "CommValue",
        ].sum()

        self._effective_interest = self.calculate_effective_interest(
            self._cash_flow_date,
            self._eir_effective.loc[last_index_eir_effective, "CashFlowDate"],
            self._eir_effective.loc[
                last_index_eir_effective, "EffectiveNotionalDue"
            ],
            self._eir_effective.loc[
                last_index_eir_effective, "EffectiveInterestRate"
            ],
        )

        self._effective_notional_due = self.calculate_effective_notional_due(
            self._eir_effective.loc[
                last_index_eir_effective, "EffectiveNotionalDue"
            ],
            self._effective_interest,
            self._comm_value,
            self._repayment_interest,
            self._repayment_capital,
        )

        self._amortization_results_effective = (
            self._effective_interest - self._repayment_accrued_interest
        )

        self._unsettled_commission_balance_effective = (
            self.calculate_unsettled_commission_balance_effective(
                self._eir_effective.loc[
                    last_index_eir_effective,
                    "UnsettledCommissionBalanceEffective",
                ],
                self._comm_value,
                self._amortization_results_effective,
            )
        )

    def create_new_row(self):
        """
        Creates new row of eir_effective and returns it as dataframe

        Returns:
            dataframe: new row of eir_effective, generated by event
        """
        self.check_calculate_eir()

        if self._calculate_eir:
            row = pd.DataFrame(
                {
                    "BusinessDate": self._business_date,
                    "EirExposureMapId": self._rrc_exp_id,
                    "CashFlowDate": self._cash_flow_date,
                    "NotionalDue": self._notional_due,
                    "RepaymentCapital": self._repayment_capital,
                    "RepaymentInterest": self._repayment_interest,
                    "RepaymentAccruedInterest": self._repayment_accrued_interest,
                    "CommValue": self._comm_value,
                    "AmortizationResultsEffective": self._amortization_results_effective,
                    "UnsettledCommissionBalanceEffective": self._unsettled_commission_balance_effective,
                    "EffectiveInterest": self._effective_interest,
                    "EffectiveNotionalDue": self._effective_notional_due,
                    "EffectiveInterestRate": self._eir_effective.loc[
                        len(self._eir_effective) - 1, "EffectiveInterestRate"
                    ]
                },
                index=[0],
            )
            if self._pre_payment:
                self._payment_schedule = self._payment_schedule_for_loadset

            cash_flow_schedule = eir_cash_flow_schedule.Eir_cash_flow_schedule(
                row, self._payment_schedule
            )
            row["EffectiveInterestRate"] = cash_flow_schedule.eir
        else:

            row = pd.DataFrame(
                {
                    "BusinessDate": self._business_date,
                    "EirExposureMapId": self._rrc_exp_id,
                    "CashFlowDate": self._cash_flow_date,
                    "NotionalDue": self._notional_due,
                    "RepaymentCapital": self._repayment_capital,
                    "RepaymentInterest": self._repayment_interest,
                    "RepaymentAccruedInterest": self._repayment_accrued_interest,
                    "CommValue": self._comm_value,
                    "AmortizationResultsEffective": self._amortization_results_effective,
                    "UnsettledCommissionBalanceEffective": self._unsettled_commission_balance_effective,
                    "EffectiveInterest": self._effective_interest,
                    "EffectiveNotionalDue": self._effective_notional_due,
                    "EffectiveInterestRate": self._eir_effective.loc[
                        len(self._eir_effective) - 1, "EffectiveInterestRate"
                    ],
                },
                index=[0],
            )

        return row

    def check_calculate_eir(self):
        # check if there is a need to calculate eir
        if "commission" in self._event_list:
            self._calculate_eir = True
        elif "change" in self._event_list:
            self._calculate_eir = True
            self._payment_schedule = self._payment_schedule_for_loadset
