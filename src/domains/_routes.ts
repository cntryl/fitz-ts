export type RouteShapeOptions = {
  allowBareRoute?: boolean;
};

export function isRouteShape(
  route: string,
  scheme: string,
  segmentCount: number,
  options: RouteShapeOptions = {},
): boolean {
  const prefix = `${scheme}://`;
  let remainder: string;

  if (route.startsWith(prefix)) {
    remainder = route.slice(prefix.length);
  } else if (options.allowBareRoute && !route.includes("://")) {
    remainder = route;
  } else {
    return false;
  }

  if (remainder.length === 0) {
    return false;
  }

  const segments = remainder.split("/");
  if (segments.length !== segmentCount) {
    return false;
  }

  return segments.every(isConcreteSegment);
}

export function isConcreteRouteShape(route: string, scheme: string): boolean {
  const prefix = `${scheme}://`;
  if (!route.startsWith(prefix)) {
    return false;
  }

  const remainder = route.slice(prefix.length);
  if (remainder.length === 0) {
    return false;
  }

  return remainder.split("/").every(isConcreteSegment);
}

export type SelectorRouteShapeOptions = {
  allowRealmWildcard?: boolean;
};

export function isSelectorRouteShape(
  route: string,
  scheme: string,
  segmentCount: number,
  options: SelectorRouteShapeOptions = {},
): boolean {
  const prefix = `${scheme}://`;
  if (!route.startsWith(prefix)) {
    return false;
  }

  const remainder = route.slice(prefix.length);
  if (remainder.length === 0) {
    return false;
  }

  const segments = remainder.split("/");
  if (segments.length === segmentCount) {
    if (segments.every(isConcreteSegment)) {
      return true;
    }

    if (segments[segmentCount - 1] === "*" && segments.slice(0, -1).every(isConcreteSegment)) {
      return true;
    }
  }

  if (options.allowRealmWildcard && segments.length === 2) {
    return isConcreteSegment(segments[0]) && segments[1] === "**";
  }

  return false;
}

function isConcreteSegment(segment: string): boolean {
  return segment.length > 0 && segment !== "*" && segment !== "**";
}
