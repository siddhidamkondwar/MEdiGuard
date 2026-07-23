"""STUB reasoner for the walking skeleton.
Builds a valid Verdict object from claim lines. No fraud detection, no LLM —
it only proves the verdict contract can be produced and validated end to end.
Real evidence computers + LLM narration replace the body later."""
from agents.schemas import Verdict, EvidenceBundle, Audit, LineAdjudication


def make_verdict(claim_id: str, lines: list[dict]) -> Verdict:
    billed_total = sum(l["billed_inr"] for l in lines)
    # Skeleton "logic": allow everything up to the HBP package rate * quantity.
    justified = 0.0
    adjudications: list[LineAdjudication] = []
    for l in lines:
        cap = (l.get("hbp_package_rate_inr") or l["unit_price_inr"]) * l["quantity"]
        allowed = min(l["billed_inr"], cap)
        justified += allowed
        status = "ALLOWED" if allowed >= l["billed_inr"] else "REDUCED"
        adjudications.append(LineAdjudication(
            line=l["line_no"], billed=l["billed_inr"], allowed=round(allowed, 2),
            status=status, reason="skeleton: capped at HBP package rate",
        ))
    excess = round(billed_total - justified, 2)

    return Verdict(
        claim_id=claim_id,
        verdict="FLAG_FOR_AUDIT" if excess > 0 else "APPROVE",
        fraud_score=0.0,                       # no model yet
        billed_total_inr=round(billed_total, 2),
        justified_total_inr=round(justified, 2),
        estimated_excess_inr=excess,
        recommended_action="HUMAN_REVIEW" if excess > 0 else "AUTO_APPROVE",
        evidence=EvidenceBundle(),             # empty in the skeleton
        line_adjudication=adjudications,
        explanation="[stub] verdict produced by walking skeleton — no ML/LLM yet.",
        audit=Audit(human_review_required=excess > 0),
    )
