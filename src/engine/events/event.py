from datetime import datetime

import pandas as pd


class Event:

    """


    class Event - as input it takes headers from table eir_effective - none of these arguments are necessary - they can be calculated later. The class is abstract
    because there are many events which need different tread. Arguments and a few methods are common

    parameters_const = {
        "exp_info" : self.exp_info,
        "commissions" : self.commissions,
        "loadset_date" : self.current_loadset_date,
        "exp_id" : self.exp_id
        }

    """

    def __init__(
        self,
        parameters_const,
        cash_flow_date,
        payment_schedule_for_init,
        nominal_interest_for_init,
    ):

        self._exp_info = parameters_const["exp_info"]
        self._commissions = parameters_const["commissions"]
        self._payment_schedule = payment_schedule_for_init
        self._business_date = parameters_const["loadset_date"]
        self._rrc_exp_id = parameters_const["exp_id"]
        self._nir = nominal_interest_for_init
        self._cash_flow_date = cash_flow_date
        self._notional_due = 0
        self._repayment_capital = 0
        self._repayment_interest = 0
        self._repayment_accrued_interest = 0
        self._comm_value = 0
        self._amortization_results_effective = 0
        self._unsettled_commission_balance_effective = 0
        self._effective_interest = 0
        self._effective_notional_due = 0
        self._effective_interest_rate = 0
        self._calculate_days_360 = parameters_const[
            "calculate_days_360"
        ]  # act/360
        self._calculate_days_30_360 = parameters_const[
            "calculate_days_30_360"
        ]  # True if days are calculated in European method (30E/360)

    def calculate(self, eir_effective):
        pass

    def create_new_row(self):
        pass

    def calc_prop_of_year(self, today, last_date):
        """calculates prop of year, if self._calculate_days_360 is True then it is calculated with 30/360 eu standard

        Args:
            today (date)
            last_date (date)

        Returns:
            float64: proportion of the year for difference between dates
        """
        if self._calculate_days_360 == True:

            return (
                pd.to_datetime(today) - pd.to_datetime(last_date)
            ).days / 360
        if self._calculate_days_30_360 == False:

            return (
                pd.to_datetime(today) - pd.to_datetime(last_date)
            ).days / 365
        else:

            today_day = today.day
            today_month = today.month
            today_year = today.year
            last_date_day = last_date.day
            last_date_month = last_date.month
            last_date_year = last_date.year

            if today_day == 31:
                today_day = 30
            if last_date_day == 31:
                last_date_day = 30

            return (
                today_day
                + today_month * 30
                + today_year * 360
                - last_date_day
                - last_date_month * 30
                - last_date_year * 360
            ) / 360

    def calculate_notional_due(
        self, last_value_of_notional_due, repayment_capital
    ):
        return last_value_of_notional_due - repayment_capital

    def calculate_interest(
        self, today_date, last_date, last_value_of_notional_due, interest_rate
    ):
        part_of_year = self.calc_prop_of_year(today_date, last_date)

        return (
            last_value_of_notional_due * (1 + interest_rate * part_of_year)
            - last_value_of_notional_due
        )

    def check_if_there_were_any_commisions(self):
        # returns true when there where any commisions before cash flow dates
        value_of_commisions_before_cash_flow_date = self._commissions.loc[
            self._commissions["CollectionDate"] < self._cash_flow_date,
            "CommValue",
        ]

        if value_of_commisions_before_cash_flow_date.empty:
            return False
        else:
            return True

    def calculate_effective_interest(
        self,
        today_date,
        last_date,
        last_effective_notional_due,
        last_effective_interest_rate,
    ):
        # default reasult, when there was no commsions
        result = self._repayment_accrued_interest
        if self.check_if_there_were_any_commisions():
            # commision in past
            part_of_year = self.calc_prop_of_year(today_date, last_date)

            result = (
                1 + last_effective_interest_rate * part_of_year
            ) * last_effective_notional_due - last_effective_notional_due
        return result

    def calculate_effective_notional_due(
        self,
        last_effective_notional_due,
        effective_interest,
        comm_value,
        interest_repayment,
        principal_repayment,
    ):
        return (
            last_effective_notional_due
            + effective_interest
            - comm_value
            - interest_repayment
            - principal_repayment
        )

    def calculate_unsettled_commission_balance_effective(
        self,
        last_unsettled_commission,
        comm_value,
        amortization_results_effective,
    ):
        print('last_unsettled_commission')
        print(last_unsettled_commission,flush=True)
        return (
            last_unsettled_commission
            + comm_value
            - amortization_results_effective
        )
