from typing import Dict, List, TypedDict

class VideoTlocTuple(TypedDict, total=False):
    videoName: str
    tlocName: str
    videoRecordedTime: int

class SessionInformation(TypedDict, total=False):
    videoCount: int
    sessionStartTime: str
    recordedUserId: str
    videoTlocTuples: List[VideoTlocTuple]