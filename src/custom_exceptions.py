class CalculationError(Exception):
    """dummy class"""

    pass


class PrevPaymentScheduleMissing(CalculationError):
    pass


class ExposureInfoMissing(CalculationError):
    pass


class PaymentScheduleMissing(CalculationError):
    pass


class OpenDateIsLaterThanBusinessDate(CalculationError):
    pass


class WrongSettlementConfiguration(CalculationError):
    pass


class WrongCollectionDate(CalculationError):
    pass
