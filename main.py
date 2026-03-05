from typing import Any, Dict, Optional

from fastapi import FastAPI

try:
    from fastapi_app.helpers import DomesticJudV2Helper, MoneyCheckHelper, RPAQueryStatus
    from fastapi_app.schemas import QueryRequest, QueryResponse
except ModuleNotFoundError:
    from helpers import DomesticJudV2Helper, MoneyCheckHelper, RPAQueryStatus
    from schemas import QueryRequest, QueryResponse


app = FastAPI(title="Judicial Query API", version="1.0.0")


def _build_response(
    status: RPAQueryStatus,
    total_num: Optional[int],
    pdf_url: Optional[str],
    raw_result: Optional[Dict[str, Any]],
) -> QueryResponse:
    if status == RPAQueryStatus.NORMAL:
        message = "Query completed: normal"
    elif status == RPAQueryStatus.ABNORMAL:
        message = "Query completed: abnormal"
    else:
        message = "Query failed"

    return QueryResponse(
        status=status.value,
        message=message,
        total_num=total_num,
        pdf_url=pdf_url,
        raw_result=raw_result,
    )


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/domestic-jud", response_model=QueryResponse)
def query_domestic_jud(req: QueryRequest) -> QueryResponse:
    helper = DomesticJudV2Helper(idnum=req.ino, name=req.name)
    status, total_num, pdf_url, raw_result = helper.get_n_check_data()
    return _build_response(status, total_num, pdf_url, raw_result)


@app.post("/debt-jud", response_model=QueryResponse)
def query_debt_jud(req: QueryRequest) -> QueryResponse:
    helper = MoneyCheckHelper(idnum=req.ino, name=req.name)
    status, total_num, pdf_url, raw_result = helper.check_debt()
    return _build_response(status, total_num, pdf_url, raw_result)


@app.post("/bankrupt-jud", response_model=QueryResponse)
def query_bankrupt_jud(req: QueryRequest) -> QueryResponse:
    helper = MoneyCheckHelper(idnum=req.ino, name=req.name)
    status, total_num, pdf_url, raw_result = helper.check_bankrupt()
    return _build_response(status, total_num, pdf_url, raw_result)

