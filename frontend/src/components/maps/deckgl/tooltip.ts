import type { PickingInfo } from '@deck.gl/core';

export function getTooltip({ object }: PickingInfo) {
  if (!object) {
    return null;
  }
  const lat = object.position[1];
  const lng = object.position[0];
  const count = object.points.length;

  return `\
    Latitudine: ${Number.isFinite(lat) ? lat.toFixed(6) : ''}
    Longitudine: ${Number.isFinite(lng) ? lng.toFixed(6) : ''}
    Conteggio: ${count}`;
}
