import pandas as pd
from src.engine.events.event import Event


class EndContract(Event):
    def __init__(
        self,
        parameters_const,
        cash_flow_date,
        payment_schedule_for_init,
        nominal_interest_for_init,
    ):

        super().__init__(
            parameters_const,
            cash_flow_date,
            payment_schedule_for_init,
            nominal_interest_for_init,
        )

    def calculate(self, eir_effective):
        self._eir_effective = eir_effective
        last_index_eir_effective = len(self._eir_effective) - 1
        
        self._repayment_capital = self._eir_effective.loc[last_index_eir_effective, "NotionalDue"]
        
        self._notional_due = self.calculate_notional_due(
            self._eir_effective.loc[last_index_eir_effective, "NotionalDue"],
            self._repayment_capital,
        )

        self._repayment_accrued_interest = self.calculate_interest(
            self._cash_flow_date,
            self._eir_effective.loc[last_index_eir_effective, "CashFlowDate"],
            self._eir_effective.loc[last_index_eir_effective, "NotionalDue"],
            self._nir,
        )

        self._effective_interest = (
            self._repayment_accrued_interest
            + self._eir_effective.loc[
                last_index_eir_effective, "UnsettledCommissionBalanceEffective"
            ]
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

        row = pd.DataFrame(
            {
                "BusinessDate": self._business_date,
                "EirExposureMapId": self._rrc_exp_id,
                "CashFlowDate": self._cash_flow_date,
                "NotionalDue": self._notional_due,
                "RepaymentCapital": self._repayment_capital,
                "RepaymentInterest": 0,
                "RepaymentAccruedInterest": self._repayment_accrued_interest,
                "CommValue": 0,
                "AmortizationResultsEffective": self._amortization_results_effective,
                "UnsettledCommissionBalanceEffective": self._unsettled_commission_balance_effective,
                "EffectiveInterest": self._effective_interest,
                "EffectiveNotionalDue": 0,
                "EffectiveInterestRate": self._eir_effective.loc[
                    len(self._eir_effective) - 1, "EffectiveInterestRate"
                ],
            },
            index=[0],
        )

        return row
