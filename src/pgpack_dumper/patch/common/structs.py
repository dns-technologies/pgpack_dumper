from enum import Enum


class ResClass(str, Enum):
    CAST = "TypeCast"
    CONST = "A_Const"
    COLUMN = "ColumnRef"
    EXPR = "A_Expr"
    FUNC = "FuncCall"
    SQLVALUE = "SQLValueFunction"
