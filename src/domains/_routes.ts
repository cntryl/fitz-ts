const maxWireRouteBytes = 65_535;
const encoder = new TextEncoder();

/**
 * Fitz clients deliberately do not parse route grammar. These compatibility
 * helpers now enforce only the shared wire-size limit; the broker owns schemes,
 * path segments, wildcard placement, and authorization.
 */
export function isRouteShape(
  route: string,
  _scheme: string,
  _segmentCount: number,
  _options: { allowBareRoute?: boolean } = {},
): boolean {
  return encoder.encode(route).byteLength <= maxWireRouteBytes;
}

export function isConcreteRouteShape(route: string, scheme: string): boolean {
  return isRouteShape(route, scheme, 0);
}

export function isSelectorRouteShape(
  route: string,
  scheme: string,
  segmentCount: number,
  _options: { allowRealmWildcard?: boolean } = {},
): boolean {
  return isRouteShape(route, scheme, segmentCount);
}
