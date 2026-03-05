from typing import Any, Optional

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    ino: str = Field(..., min_length=1, description="Identity number")
    name: str = Field(..., min_length=1, description="Name")


class QueryResponse(BaseModel):
    status: str = Field(..., description="Y=正常, N=異常, ERR=錯誤")
    message: str = Field(..., description="說明文字")
    total_num: Optional[int] = Field(None, description="查詢筆數")
    pdf_url: Optional[str] = Field(None, description="司法院回傳之 PDF 網址")
    raw_result: Optional[dict[str, Any]] = Field(None, description="原始查詢結果")

