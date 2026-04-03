
from typing import TypedDict


class InterestingnessFilter(TypedDict):
    min_views: int
    view_weight: float
    min_faves: int
    fave_weight: float
    min_comments: int
    comment_weight: float
