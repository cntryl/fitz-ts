export type RouteShapeOptions = { allowBareRoute?: boolean };
export type SelectorRouteShapeOptions = { allowRealmWildcard?: boolean };

const maxWireBytes = 65_535;

export function isRouteShape(
  route: string,
  scheme: string,
  segmentCount: number,
  options?: RouteShapeOptions,
): boolean {
  const start = pathStart(route, scheme, options?.allowBareRoute === true);
  return start >= 0 && scanConcrete(route, start, segmentCount);
}

export function isConcreteRouteShape(route: string, scheme: string): boolean {
  const start = pathStart(route, scheme, false);
  return start >= 0 && scanConcrete(route, start, 0);
}

export function isSelectorRouteShape(
  route: string,
  scheme: string,
  segmentCount: number,
  options?: SelectorRouteShapeOptions,
): boolean {
  const start = pathStart(route, scheme, false);
  if (start < 0) return false;

  let count = 0;
  let firstWildcard = -1;
  let hasDoubleWildcard = false;
  let wildcardSuffix = true;
  let segmentStart = start;

  for (let index = start; index <= route.length; index++) {
    if (index !== route.length && route.charCodeAt(index) !== 47) continue;
    const length = index - segmentStart;
    if (length === 0) return false;
    const single = length === 1 && route.charCodeAt(segmentStart) === 42;
    const double =
      length === 2 &&
      route.charCodeAt(segmentStart) === 42 &&
      route.charCodeAt(segmentStart + 1) === 42;
    if (single || double) {
      if (firstWildcard < 0) firstWildcard = count;
      hasDoubleWildcard ||= double;
    } else {
      if (containsAsterisk(route, segmentStart, index)) return false;
      if (firstWildcard >= 0) wildcardSuffix = false;
    }
    count++;
    segmentStart = index + 1;
  }

  if (
    options?.allowRealmWildcard === true &&
    count === 2 &&
    firstWildcard === 1 &&
    hasDoubleWildcard
  )
    return true;
  if (count !== segmentCount || firstWildcard === 0) return false;
  if (firstWildcard < 0) return true;
  if (hasDoubleWildcard || !wildcardSuffix) return false;
  return (
    firstWildcard === segmentCount - 1 ||
    (options?.allowRealmWildcard === true && firstWildcard === 1)
  );
}

function pathStart(route: string, scheme: string, allowBare: boolean): number {
  if (!wireSizeIsValid(route)) return -1;
  const schemeLength = scheme.length;
  if (
    route.length > schemeLength + 3 &&
    route.startsWith(scheme) &&
    route.charCodeAt(schemeLength) === 58 &&
    route.charCodeAt(schemeLength + 1) === 47 &&
    route.charCodeAt(schemeLength + 2) === 47
  ) {
    return schemeLength + 3;
  }
  return allowBare && !route.includes("://") && route.length > 0 ? 0 : -1;
}

function scanConcrete(route: string, start: number, expectedSegments: number): boolean {
  let count = 0;
  let segmentStart = start;
  for (let index = start; index <= route.length; index++) {
    if (index !== route.length && route.charCodeAt(index) !== 47) continue;
    if (index === segmentStart || containsAsterisk(route, segmentStart, index)) return false;
    count++;
    segmentStart = index + 1;
  }
  return count > 0 && (expectedSegments === 0 || count === expectedSegments);
}

function containsAsterisk(route: string, start: number, end: number): boolean {
  for (let index = start; index < end; index++) {
    if (route.charCodeAt(index) === 42) return true;
  }
  return false;
}

function wireSizeIsValid(route: string): boolean {
  if (route.length > maxWireBytes) return false;
  let bytes = route.length;
  for (let index = 0; index < route.length; index++) {
    const code = route.charCodeAt(index);
    if (code < 0x80) continue;
    if (code < 0x800) bytes++;
    else if (
      code >= 0xd800 &&
      code <= 0xdbff &&
      index + 1 < route.length &&
      route.charCodeAt(index + 1) >= 0xdc00 &&
      route.charCodeAt(index + 1) <= 0xdfff
    ) {
      bytes += 2;
      index++;
    } else bytes += 2;
    if (bytes > maxWireBytes) return false;
  }
  return true;
}
