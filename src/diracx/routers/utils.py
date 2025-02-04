__all__ = ("has_properties",)

from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import SecurityProperty, UnevaluatedProperty

from .auth import UserInfo, verify_dirac_token


def has_properties(expression: UnevaluatedProperty | SecurityProperty):
    evaluator = (
        expression
        if isinstance(expression, UnevaluatedProperty)
        else UnevaluatedProperty(expression)
    )

    async def require_property(user: Annotated[UserInfo, Depends(verify_dirac_token)]):
        if not evaluator(user.properties):
            raise HTTPException(status.HTTP_403_FORBIDDEN)

    return Depends(require_property)
