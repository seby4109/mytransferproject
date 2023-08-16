import pandas as pd
from src.engine.creator_settelements import CreatorSettelments
from src.engine.generate_event_list import GenerateEvents


class EffectiveCalc:
    def __init__(
        self,
        loadset_date,
        id,
        exp_info,
        commissions,
        payment_schedule,
        eir_effective,
        commission_settlement,
        is_new,
        effective_settelment,
        is_end,
    ):
        """_summary_

        Args:
            loadset_date (date):
            id (int?):
            exp_info (daataframe):
            commissions (daataframe):
            payment_schedule (daataframe):
            eir_effective (daataframe):
            commission_settlement (daataframe):
            is_new (bool):
            effective_settelment (daataframe):
        """
        self._loadset_date = loadset_date
        self._exp_info = exp_info
        self._commissions = commissions
        self._eir_effective = eir_effective

        self._commission_settlement = commission_settlement
        self._is_new = is_new
        self._is_end = is_end
        self._id = id
        self._effective_settelment = effective_settelment

        if is_end:
            self.create_data_for_end()

        if is_new:
            self._prev_loadset_date = None
            self._prev_payment_schedule = None
        else:
            self._prev_loadset_date = eir_effective["BusinessDate"].max()
            self._prev_payment_schedule = payment_schedule[
                payment_schedule["BusinessDate"] == self._prev_loadset_date
            ]

        self._payment_schedule_now = payment_schedule[
            payment_schedule["BusinessDate"] == loadset_date
        ]

    def calculate(self):
        events, dateframe_events = GenerateEvents(
            loadset_date=self._loadset_date,
            prev_loadset_date=self._prev_loadset_date,
            exp_info=self._exp_info,
            new_exposure=self._is_new,
            prev_payment_schedule=self._prev_payment_schedule,
            payment_schedule=self._payment_schedule_now,
            commissions=self._commissions,
            eir_effective=self._eir_effective,
            is_end=self._is_end,
        ).generate_events()

        for event in events:
            event.calculate(self._eir_effective)
            row = event.create_new_row()
            self._eir_effective = pd.concat(
                [self._eir_effective, row],
                ignore_index=True,
            )

        cs = CreatorSettelments(
            self._loadset_date, self._commissions, self._id, self._eir_effective
        )

        rows_effective = cs.generate_rows_EffectiveSettelment(
            self._effective_settelment
        )

        rows_commission = cs.calulate_commission_settlement(
            dateframe_events, self._commission_settlement
        )
        rows_eir_effective = self._eir_effective.loc[
            self._eir_effective["BusinessDate"] == self._loadset_date, :
        ]

        return (
            rows_effective,
            rows_commission,
            rows_eir_effective,
        )

    def create_data_for_end(self):
        copy = self._exp_info.copy()
        copy["BusinessDate"] = self._loadset_date

        self._exp_info = pd.concat([self._exp_info, copy], ignore_index=True)
