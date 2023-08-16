import pandas as pd
import random
from src.engine.core_functions import Core
import os

class Eir_cash_flow_schedule:
    def __init__(self, eir_effective, payment_schedule):

        """_summary_
        In:
            eir_effective: data frame, last values in data are used to
                build first row of cash flow
            payment_schedule: data frame, correct paymant schedule for
                given date is needed
        Out:
            df (dataframe) with columns:
                LoadsetDate
                EirCashFlowScheduleId
                CashFlowDate
                PrincipalRepaymentAmountCCY
                PrincipalRepaymentAmountLCY
                InterestRepaymentAmountCCY
                InterestRepaymentAmountLCY
                CommissionAmountCCY
                CommissionAmountLCY
                EffectiveCashFlowAmountCCY
                EffectiveCashFlowAmountLCY

            eir (number): EffectiveInterestRate
        """
        self.data = self.create_table(eir_effective, payment_schedule)
        self.eir = Core.xirr(self.data['CashFlowDate'].to_numpy().astype('datetime64[D]'),
                             self.data['EffectiveCashFlowAmountCCY'].to_numpy())
        self.id = self.data['EirCashFlowScheduleId'].iloc[-1]

    def create_table(self, eir_effective, payment_schedule):
        # date of cashflow from eir effective
        cash_flow_date_from_eir_effective = eir_effective['CashFlowDate'].iloc[-1]

        # payment schedule after date of last cash flow from eir effective

        payment_schedule = payment_schedule[payment_schedule['Date'].to_numpy().astype('datetime64[D]') > eir_effective.loc[len(eir_effective)-1, 'CashFlowDate']]

        # first row of data is from eir effective
        seed = os.urandom(100)
        random.seed(seed)
        df1 = pd.DataFrame({
            'LoadsetDate':  eir_effective['BusinessDate'].iloc[-1],
            'EirCashFlowScheduleId': random.randint(1,1000000),
            'CashFlowDate': cash_flow_date_from_eir_effective,
            'PrincipalRepaymentAmountCCY': -1 * eir_effective['NotionalDue'].iloc[-1],
            'InterestRepaymentAmountCCY': eir_effective['RepaymentInterest'].iloc[-1],
            'CommissionAmountCCY': eir_effective['CommValue'].iloc[-1],
            'EffectiveCashFlowAmountCCY': -1 * eir_effective['EffectiveNotionalDue'].iloc[-1],
            },
            index=[0])

        # rest is from payment schedule without payments that are before date
        # of first row
        df2 = pd.DataFrame({
            'LoadsetDate':  eir_effective['BusinessDate'].iloc[-1],
            'EirCashFlowScheduleId': [df1.loc[0,'EirCashFlowScheduleId'] for i in range(len(payment_schedule))],
            'CashFlowDate': payment_schedule['Date'],
            'PrincipalRepaymentAmountCCY': payment_schedule['Capital'],
            'InterestRepaymentAmountCCY': payment_schedule['Interest'],
            'CommissionAmountCCY': 0,
        })

        # creating two columns that values are sum of other columns
        df2['EffectiveCashFlowAmountCCY'] = df2['PrincipalRepaymentAmountCCY'] + df2['InterestRepaymentAmountCCY'] + df2['CommissionAmountCCY']
        return pd.concat([df1, df2])