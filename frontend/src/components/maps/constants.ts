// Map configuration
export const INITIAL_VIEW_STATE = {
  longitude: 12.4964,
  latitude: 41.9028,
  zoom: 12.7, // Reduced zoom level to show more area
  pitch: 45,
  bearing: 0
} as const;

export const INITIAL_VIEW_STATE_CLUSTER = {
  longitude: 12.4964,
  latitude: 41.9028,
  zoom: 10, // Reduced zoom level to show more area
} as const;

export const INITIAL_VIEW_STATE_DISTANCES = {
  longitude: 12.4964,
  latitude: 41.9028,
  pitch: 60,
  zoom: 11,
} as const;

export const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

// Color range from DeckGL implementation
export const COLOR_RANGE = [
  [0, 128, 255],    // Light blue
  [0, 176, 255],    // Medium blue
  [0, 221, 255],    // Cyan blue
  [255, 196, 0],    // Yellow
  [255, 128, 0],    // Orange
  [255, 0, 0]       // Red
] as const;

