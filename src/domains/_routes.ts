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

  return segments.every((segment) => segment.length > 0 && segment !== "*" && segment !== "**");
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

  return remainder.split("/").every((segment) => segment.length > 0 && segment !== "*" && segment !== "**");
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
    if (segments.every((segment) => segment.length > 0 && segment !== "*" && segment !== "**")) {
      return true;
    }

    if (
      segments[segmentCount - 1] === "*" &&
      segments.slice(0, -1).every((segment) => segment.length > 0 && segment !== "*" && segment !== "**")
    ) {
      return true;
    }
  }

  if (options.allowRealmWildcard && segments.length === 2) {
    return segments[0].length > 0 && segments[0] !== "*" && segments[0] !== "**" && segments[1] === "**";
  }

  return false;
}
