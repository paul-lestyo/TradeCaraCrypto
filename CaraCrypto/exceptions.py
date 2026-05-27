# Tujuan
# Kumpulan exception domain trading.
# Caller
# Service layer.
# Dependensi
# builtins Exception.
# Main Functions
# Menyediakan tipe error spesifik.
# Side Effects
# Tidak ada.

class TradingError(Exception):
    pass


class ParseError(TradingError):
    pass


class ExtractionError(ParseError):
    pass


class OrderExecutionError(TradingError):
    pass


class RiskLimitError(TradingError):
    pass


class PositionNotFoundError(TradingError):
    pass


class ConnectionError(TradingError):
    pass
