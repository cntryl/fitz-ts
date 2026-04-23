export type RouteShapeOptions = {
  allowBareRoute?: boolean;
};

export function isRouteShape(
  route: string,
  _scheme: string,
  _segmentCount: number,
  _options: RouteShapeOptions = {},
): boolean {
  // Route strings are opaque protocol inputs; semantic validation is broker-owned.
  return typeof route === "string";
}

export function isConcreteRouteShape(route: string, _scheme: string): boolean {
  return typeof route === "string";
}

export type SelectorRouteShapeOptions = {
  allowRealmWildcard?: boolean;
};

export function isSelectorRouteShape(
  route: string,
  _scheme: string,
  _segmentCount: number,
  _options: SelectorRouteShapeOptions = {},
): boolean {
  return typeof route === "string";
}
