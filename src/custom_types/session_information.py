from typing import Dict, List, TypedDict

class VideoTlocTuple(TypedDict, total=False):
    videoName: str
    tlocName: str

class SessionInformation(TypedDict, total=False):
    videoCount: int
    sessionStartTime: str
    videoTlocTuples: List[VideoTlocTuple]