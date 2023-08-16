import pandas as pd

from src import custom_exceptions
from src.engine.events import (
    commission,
    end_of_contract,
    loadset,
    new_credit,
    new_credit_commission,
    repayment,
)
from src.utils import error_location


class GenerateEvents:
    def __init__(
        self,
        loadset_date,
        prev_loadset_date,
        exp_info,
        new_exposure,
        prev_payment_schedule,
        payment_schedule,
        commissions,
        eir_effective,
        is_end,
    ):
        """
        Args:
            loadset_date (date?): date of this loadset
            prev_loadset_date (date?): date of previous loadset, if there is no such date - it is None
            exp_info (dataframe): table exp_info with 2 or 1 row - (1 - last loadset, 2 - this loadset)
            new_exposure (dataframe)
            prev_payment_schedule (dataframe): payment schedule with prev loadset date
            payment_schedule (dataframe): payment schedule with this loadset date
            commissions (dataframe)
            eir_effective (dataframe)
        """
        if exp_info.empty:
            raise custom_exceptions.ExposureInfoMissing(f"{error_location()}")
        if (not is_end) and payment_schedule.empty:
            raise custom_exceptions.PaymentScheduleMissing(
                f"{error_location()}"
            )
        if (not new_exposure) and prev_payment_schedule.empty:
            raise custom_exceptions.PrevPaymentScheduleMissing(
                f"{error_location()}"
            )

        self.exp_info = exp_info
        self.payment_schedule = payment_schedule
        self.commissions = commissions
        self.eir_effective = eir_effective
        self.new_exposure = new_exposure
        self.is_end = is_end

        if prev_payment_schedule is None:
            self.prev_payment_schedule = payment_schedule
        else:
            self.prev_payment_schedule = prev_payment_schedule

        self.current_loadset_date = loadset_date

        if prev_loadset_date is None:
            self.prev_loadset_date = pd.to_datetime(
                self.exp_info["OpenDate"], format="%Y-%m-%d"
            ).loc[
                0,
            ]
        else:
            self.prev_loadset_date = prev_loadset_date

        self.exp_id = self.exp_info.loc[0, "EirExposureMapId"]

        self.dict_of_events = (
            {}
        )  # dictionary of events, where key is date of event, and value is string

    def check_new_contract(self):
        """
        checks if from last loadset date there was event of new contract
        returns [(event_name, date)]
        event_name - object of type event
        date - date of occuring of event
        """
        if self.new_exposure:
            dt = self.exp_info["OpenDate"].loc[
                0,
            ]
            self.add_to_dict_of_events(dt, "new")

    def check_payments(self):
        """
        checks if from last date of loadset there were some payments from payment_schedule
        returns [(event_name, date), (event_name, date), (event_name, date)]
        event_name - object of type event
        date - date of occuring of event
        """
        events = self.prev_payment_schedule[
            (self.prev_payment_schedule["Date"] > self.prev_loadset_date)
            & (self.prev_payment_schedule["Date"] <= self.current_loadset_date)
        ]["Date"]

        for event in events:
            self.add_to_dict_of_events(event, "pay")

    def check_costs_and_provisions(self):
        """
        checks if from last date of loadset there were any of new costs or provisions
        returns [(event_name, date), (event_name, date), (event_name, date)]
        event_name - object of type event
        date - date of occuring of event
        """
        # all commision loaded in this most recent load date
        commissions_after_filter = self.commissions.loc[
            self.commissions["CommValue"] != 0
        ]

        dates_of_commisions = commissions_after_filter.loc[
            self.commissions["BusinessDate"] >= self.current_loadset_date,
            "CollectionDate",
        ]

        for date in dates_of_commisions:
            if not self.new_exposure and date <= self.prev_loadset_date:
                raise custom_exceptions.WrongCollectionDate(
                    f"Commission collection date {date} ",
                    f"should be later than {self.prev_loadset_date}. "
                    f"{error_location()}",
                )
            elif self.new_exposure and date < self.prev_loadset_date:
                raise custom_exceptions.WrongCollectionDate(
                    f"Commission collection date {date} ",
                    f"should be equal or later than {self.prev_loadset_date}. "
                    f"{error_location()}",
                )
            self.add_to_dict_of_events(date, "commission")
            # event_list.append((event, date))

    def check_payment_schedule_and_nominal_interest(self):
        """
        checks if from last date of loadset there were any changes in payment schedule or change in nominal interest
        returns [(event_name, date), (event_name, date), (event_name, date)]
        event_name - object of type event
        date - date of occuring of event
        """

        # check if there is a difference in nominal interest rate or contract close date between two loadset dates
        if len(self.exp_info) > 1:
            if (
                self.exp_info.loc[
                    self.exp_info["BusinessDate"] == self.prev_loadset_date,
                    "Nir",
                ].values[0]
                != self.exp_info.loc[
                    self.exp_info["BusinessDate"] == self.current_loadset_date,
                    "Nir",
                ].values[0]
                or self.exp_info.loc[
                    self.exp_info["BusinessDate"] == self.prev_loadset_date,
                    "MaturityDate",
                ].values[0]
                != self.exp_info.loc[
                    self.exp_info["BusinessDate"] == self.current_loadset_date,
                    "MaturityDate",
                ].values[0]
            ):
                """
                Date of event is chosen based on the last payment before
                current loadset date and next the date is postponed by one day
                """
                date_of_schedule_change = max(
                    self.prev_payment_schedule.loc[
                        (
                            self.prev_payment_schedule["Date"]
                            < self.current_loadset_date
                        ),
                        "Date",
                    ]
                ) + pd.DateOffset(days=1)

                self.add_to_dict_of_events(date_of_schedule_change, "change")

    def check_end_of_contract(self):

        """
        checks if from last date of loadset there was the end of the contract
        """

        if (
            self.exp_info["MaturityDate"].iloc[-1] <= self.current_loadset_date
            and self.exp_info["MaturityDate"].iloc[-1] > self.prev_loadset_date
        ):
            date_of_end = self.exp_info["MaturityDate"].iloc[-1]
        if (
            self.exp_info["MaturityDate"].iloc[-1] > self.current_loadset_date
            and self.exp_info["BusinessDate"].iloc[-1] + pd.DateOffset(month=1)
            < self.current_loadset_date
        ):
            date_of_end = self.exp_info["BusinessDate"].iloc[-1]

        self.add_to_dict_of_events(date_of_end, "end")

    def init_event(
        self,
        params,
        event_type,
        cash_flow_date,
        payment_schedule_for_init,
        payment_schedule_for_loadset,
        nominal_interest_for_init,
    ):
        """
        Creates correct object of event type

        IN:
            params - dictionary of parameners needed to init event
            event_type - string of event type:
                - new contract == "new"
                - commission == "commission"
                - payment == "pay"
                - change in NIR or schedule = "change"
            cash_flow_date - time stamp of cash flow
            payment_schedule_for_init - dataframe of payment schedule that is needed for init
            nominal_interest_for_init - right value of NIR for this event
        """
        if event_type == "pay":

            return repayment.Repayment(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )

        elif event_type == "commission" or event_type == "change":

            return commission.Commission(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )

        elif event_type == "new, Loadset" or event_type == "new":

            return new_credit.New_credit(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )
        elif "commission" in event_type and "new" in event_type:
            # there is commission in date of creating exp
            return new_credit_commission.NewAndCommission(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )

        elif "Loadset" in event_type:
            return loadset.Loadset(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                payment_schedule_for_loadset,
                nominal_interest_for_init,
                event_type,
            )
        elif "end" in event_type:
            return end_of_contract.EndContract(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )

        else:
            # more than one event on date
            return commission.Commission(
                params,
                cash_flow_date,
                payment_schedule_for_init,
                nominal_interest_for_init,
            )

    def add_to_dict_of_events(self, cash_flow_date, event):
        """
        IN:
            event_type - string of event type:
                - new contract == "new"
                - commission == "commission"
                - payment == "pay"
                - change in NIR or schedule = "change"
            cash_flow_date - time stamp of cash flow

        Adds to dict of events, if on this date there is another event
        then it is merged to one
        """

        if self.dict_of_events.get(cash_flow_date) is None:
            self.dict_of_events[cash_flow_date] = event

        else:
            # there was event in this date
            self.dict_of_events[cash_flow_date] = (
                self.dict_of_events.get(cash_flow_date) + ", " + event
            )

    def generate_events(self):
        """
        creates list of sorted by date initialized events
        returns tuple [event_object, ... event_object], df_events
        event_object - object of type event
        date - date of occuring of event
        df_events - dataframe that have dates and names of events that happend on this date
        """
        if self.is_end:
            self.check_payments()
            self.check_end_of_contract()
        else:
            # check all events and add them to dict of events (it is done to deal with events on same date)
            self.check_new_contract()
            self.check_costs_and_provisions()
            self.check_payment_schedule_and_nominal_interest()
            self.check_payments()
            self.add_to_dict_of_events(self.current_loadset_date, "Loadset")

        # create list of tuples - [("event_type", date), ... ("event_type", date)]
        event_list = list(
            zip(
                self.dict_of_events.values(),
                self.dict_of_events.keys(),
                [self.exp_id] * len(self.dict_of_events.values()),
            )
        )
        print('EVENTS')
        print(event_list,flush=True)

        # sort list by date
        event_list = sorted(event_list, key=lambda t: t[1])

        if self.exp_info["AccrualConventionType"].iloc[-1] == 2:
            calculate_days_360 = True
        else:
            calculate_days_360 = False

        # dict of parameters that are const for all events
        parameters_const = {
            "exp_info": self.exp_info,
            "commissions": self.commissions,
            "loadset_date": self.current_loadset_date,
            "exp_id": self.exp_id,
            "calculate_days_30_360": False,  # TODO: dla kogoś kto sobie to potem bd robił to trzeba tu dać True jak się chce liczyć 30_360 dni
            "calculate_days_360": calculate_days_360,
        }

        # variables that can change for some events
        payment_schedule_for_init = (
            self.prev_payment_schedule
        )  # must be payment schedule from last loadset (if exist, else from this loadset)
        nominal_interest_for_init = self.exp_info.loc[
            0, "Nir"
        ]  # value of NIR from last loadset
        payment_schedule_for_loadset = self.payment_schedule

        list_of_ready_events = []

        for tuple in event_list:

            event_type = tuple[0]
            cash_flow_date = tuple[1]

            # check if this event changes payment schedule or nominal interest
            if "change" in event_type:

                payment_schedule_for_init = (
                    self.payment_schedule
                )  # most recent payment schedule
                nominal_interest_for_init = self.exp_info.loc[
                    len(self.exp_info) - 1, "Nir"
                ]  # most recent NIR

            list_of_ready_events.append(
                self.init_event(
                    parameters_const,
                    event_type,
                    cash_flow_date,
                    payment_schedule_for_init,
                    payment_schedule_for_loadset,
                    nominal_interest_for_init,
                )
            )

        return list_of_ready_events, pd.DataFrame(
            event_list, columns=["event_type", "date", "exp_id"]
        )

    """
    Next step in main class:


    generator = GenerateEvents(loadset, exp_info, new_exposure, payment_schedule, commissions)

    list_of_events = generator.generate_events()

    for event in l:
        event.calculate(eir_effective)
        # get new row of eir_effective 
        row = event.calculate_from_eir()
        eir_effective = pd.concat([eir_effective, row], ignore_index = True)
    """
