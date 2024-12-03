from typing import TypedDict, List


class GeoCoordinate(TypedDict, total=False):
    lat: float
    lng: float

class Asset(TypedDict, total=False):
    assetTypeId: str
    geoCoordinate: GeoCoordinate
    imageFileName: str
    recordedAt: str


class AssetsPayload(TypedDict, total=False):
    recordedUserId: str
    assets : List[Asset]