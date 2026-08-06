export type RouteShapeOptions = { allowBareRoute?: boolean };

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

export function isRegistrationPatternShape(
  route: string,
  scheme: string,
  requiredSegments = 0,
): boolean {
  const start = pathStart(route, scheme, false);
  if (start < 0 || requiredSegments < 0) return false;

  let segmentCount = 0;
  let doubleWildcardCount = 0;
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
    if (!single && !double && containsAsterisk(route, segmentStart, index)) return false;
    if (double) doubleWildcardCount++;
    segmentCount++;
    segmentStart = index + 1;
  }

  if (requiredSegments === 0) return true;
  if (doubleWildcardCount === 0) return segmentCount === requiredSegments;
  return segmentCount - doubleWildcardCount <= requiredSegments;
}

export function isStreamSelectorShape(route: string): boolean {
  const start = pathStart(route, "stream", false);
  if (start < 0) return false;
  if (new TextEncoder().encode(route).length > 512) return false;
  if (route === "stream://**") return true;
  const segments = route.slice(start).split("/");
  if (
    segments.length === 2 &&
    segments[1] === "**" &&
    segments[0] !== undefined &&
    segments[0] !== "*" &&
    !segments[0].includes("*")
  )
    return true;
  if (segments.length !== 3 || segments.some((segment) => segment.length === 0)) return false;
  const realm = segments[0]!;
  const area = segments[1]!;
  const resource = segments[2]!;
  const literal = (s: string) => !s.includes("*");
  const wild = (s: string) => s === "*";
  return (
    (literal(realm) && literal(area) && literal(resource)) ||
    (literal(realm) && literal(area) && wild(resource)) ||
    (literal(realm) && wild(area) && wild(resource))
  );
}

export function routeMatchesPattern(route: string, pattern: string): boolean {
  const routeMarker = route.indexOf("://");
  const patternMarker = pattern.indexOf("://");
  if (
    routeMarker < 1 ||
    patternMarker < 1 ||
    route.slice(0, routeMarker) !== pattern.slice(0, patternMarker)
  )
    return false;
  const routeSegments = routeSegmentsOf(route);
  const patternSegments = routeSegmentsOf(pattern);
  if (!routeSegments || !patternSegments) return false;

  let routeIndex = 0;
  let patternIndex = 0;
  let lastDoubleWildcard = -1;
  let lastDoubleMatch = 0;
  while (routeIndex < routeSegments.length) {
    const segment = patternSegments[patternIndex];
    if (segment === "*" || segment === routeSegments[routeIndex]) {
      routeIndex++;
      patternIndex++;
      continue;
    }
    if (segment === "**") {
      lastDoubleWildcard = patternIndex++;
      lastDoubleMatch = routeIndex;
      continue;
    }
    if (lastDoubleWildcard < 0) return false;
    routeIndex = ++lastDoubleMatch;
    patternIndex = lastDoubleWildcard + 1;
  }
  while (patternSegments[patternIndex] === "**") patternIndex++;
  return patternIndex === patternSegments.length;
}

function routeSegmentsOf(route: string): string[] | undefined {
  const marker = route.indexOf("://");
  return marker < 1 ? undefined : route.slice(marker + 3).split("/");
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
