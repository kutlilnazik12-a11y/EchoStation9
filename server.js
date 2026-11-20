/**
 * ЭХО СТАНЦИИ: ОБЪЕКТ-9 — Компактный авторитетный сервер (1 файл, 0 внешних зависимостей).
 * Одновременно раздаёт index.html и обслуживает реальный WebSocket-мультиплеер.
 */
import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';


// === SHARED: protocol.js ===
/**
 * Общий сетевой протокол клиента и сервера «ЭХО СТАНЦИИ: ОБЪЕКТ-9».
 */

const PROTOCOL_VERSION = '1.1.0';

const LIMITS = {
  MAX_PLAYERS_DEFAULT: 4,
  MAX_PLAYERS_CAP: 6,
  MIN_PLAYERS_ONLINE: 1,
  MAX_NAME_LENGTH: 18,
  MAX_ROOM_NAME_LENGTH: 28,
  MAX_CHAT_LENGTH: 160,
  MAX_MOVE_SPEED: 6.2,
  INTERACT_RANGE: 3.3,
  PLAYER_INTERACT_RANGE: 3.6,
  RECONNECT_GRACE_MS: 45_000,
  SERVER_TICK_HZ: 15,
  CLIENT_SEND_HZ: 15,
};

const MSG = {
  // Клиент -> Сервер (C2S)
  HELLO: 'c2s:hello',
  PING: 'c2s:ping',
  LIST_ROOMS: 'c2s:list_rooms',
  CREATE_ROOM: 'c2s:create_room',
  JOIN_ROOM: 'c2s:join_room',
  JOIN_BY_CODE: 'c2s:join_by_code',
  RECONNECT: 'c2s:reconnect',
  LEAVE_ROOM: 'c2s:leave_room',
  SET_READY: 'c2s:set_ready',
  UPDATE_PROFILE: 'c2s:update_profile',
  START_GAME: 'c2s:start_game',
  RETURN_LOBBY: 'c2s:return_lobby',
  PLAYER_STATE: 'c2s:player_state',
  INTERACT_OBJECT: 'c2s:interact_object',
  INTERACT_PLAYER: 'c2s:interact_player',
  SUBMIT_CODE: 'c2s:submit_code',
  CHAT_SEND: 'c2s:chat_send',
  RADIO_PING: 'c2s:radio_ping',

  // Сервер -> Клиент (S2C)
  WELCOME: 's2c:welcome',
  PONG: 's2c:pong',
  ERROR: 's2c:error',
  ROOM_LIST: 's2c:room_list',
  ROOM_JOINED: 's2c:room_joined',
  ROOM_LEFT: 's2c:room_left',
  LOBBY_STATE: 's2c:lobby_state',
  GAME_STARTED: 's2c:game_started',
  WORLD_SNAPSHOT: 's2c:world_snapshot',
  OBJECT_UPDATED: 's2c:object_updated',
  OPEN_KEYPAD: 's2c:open_keypad',
  PLAYER_ACTION: 's2c:player_action',
  CHAT_MESSAGE: 's2c:chat_message',
  RADIO_SIGNAL: 's2c:radio_signal',
  HORROR_EVENT: 's2c:horror_event',
  SESSION_ENDED: 's2c:session_ended',
  SYSTEM_NOTICE: 's2c:system_notice',
};

const ROOM_STATUS = {
  LOBBY: 'lobby',
  PLAYING: 'playing',
  ENDED: 'ended',
};

const EVENT_SCOPE = {
  GLOBAL: 'global',
  INDIVIDUAL: 'individual',
  DYNAMIC: 'dynamic',
};

// === SHARED: mapData.js ===
/**
 * Авторитетная карта подземной акустической станции «ОБЪЕКТ-9».
 * Включает: стены, зоны, двери, реле, случайные точки ключ-карты, 4 терминала с фрагментами кода,
 * проверку коллизий и 2D-рейкаст линии видимости (защита от нажатий и спавнов сквозь стены).
 */

const MAP_CONFIG = {
  name: 'Подземная станция «Объект-9»',
  ceilingHeight: 3.6,
  worldBounds: { minX: -28, maxX: 28, minZ: -28, maxZ: 28 },
  playerRadius: 0.42,
  requiredRelays: 4,
};

const ZONES = [
  { id: 'hub', name: 'Центральный узел связи', minX: -7, maxX: 7, minZ: -7, maxZ: 7 },
  { id: 'north_corridor', name: 'Северный шлюзовой коридор', minX: -3, maxX: 3, minZ: -20, maxZ: -7 },
  { id: 'airlock', name: 'Шлюз эвакуации «Альфа»', minX: -6, maxX: 6, minZ: -27, maxZ: -20 },
  { id: 'west_wing', name: 'Западная лаборатория акустики', minX: -24, maxX: -7, minZ: -8, maxZ: 8 },
  { id: 'east_wing', name: 'Восточный генераторный блок', minX: 7, maxX: 24, minZ: -8, maxZ: 8 },
  { id: 'south_archive', name: 'Южный архив записей', minX: -9, maxX: 9, minZ: 7, maxZ: 24 },
];

const SPAWN_POINTS = [
  { x: -2.2, z: -1.8, yaw: 0.6 },
  { x: 2.2, z: -1.8, yaw: -0.6 },
  { x: -2.2, z: 2.0, yaw: 2.4 },
  { x: 2.2, z: 2.0, yaw: -2.4 },
  { x: 0.0, z: 3.2, yaw: 3.14 },
  { x: 0.0, z: -3.2, yaw: 0.0 },
];

// Возможные случайные места спавна ключ-карты от Южного архива
const KEYCARD_SPAWNS = [
  { x: -22.2, z: 6.2, zoneLabel: 'Западная лаборатория (дальний угол)' },
  { x: -11.5, z: -6.4, zoneLabel: 'Западная лаборатория (северный пост)' },
  { x: 18.0, z: -6.2, zoneLabel: 'Генераторный блок (турбина А)' },
  { x: 11.2, z: 6.2, zoneLabel: 'Генераторный блок (южный щиток)' },
];

const WALLS = [
  { id: 'outer_n', x: 0, z: -27.5, w: 14, d: 1 },
  { id: 'outer_nw_1', x: -6.5, z: -23.5, w: 1, d: 7 },
  { id: 'outer_ne_1', x: 6.5, z: -23.5, w: 1, d: 7 },

  { id: 'nc_w', x: -3.5, z: -13.5, w: 1, d: 13 },
  { id: 'nc_e', x: 3.5, z: -13.5, w: 1, d: 13 },

  { id: 'hub_nw', x: -5.5, z: -7.5, w: 4, d: 1 },
  { id: 'hub_ne', x: 5.5, z: -7.5, w: 4, d: 1 },
  { id: 'hub_sw', x: -5.5, z: 7.5, w: 4, d: 1 },
  { id: 'hub_se', x: 5.5, z: 7.5, w: 4, d: 1 },

  { id: 'west_n', x: -16, z: -8.5, w: 18, d: 1 },
  { id: 'west_s', x: -16, z: 8.5, w: 18, d: 1 },
  { id: 'west_far', x: -24.5, z: 0, w: 1, d: 18 },
  { id: 'west_div_n', x: -15, z: -5.2, w: 0.8, d: 5.5 },
  { id: 'west_div_s', x: -15, z: 5.2, w: 0.8, d: 5.5 },
  { id: 'hub_w_n', x: -7.5, z: -4.8, w: 1, d: 5.4 },
  { id: 'hub_w_s', x: -7.5, z: 4.8, w: 1, d: 5.4 },

  { id: 'east_n', x: 16, z: -8.5, w: 18, d: 1 },
  { id: 'east_s', x: 16, z: 8.5, w: 18, d: 1 },
  { id: 'east_far', x: 24.5, z: 0, w: 1, d: 18 },
  { id: 'gen_col_1', x: 14, z: -3.5, w: 2.2, d: 2.2 },
  { id: 'gen_col_2', x: 14, z: 3.5, w: 2.2, d: 2.2 },
  { id: 'gen_col_3', x: 20, z: 0, w: 2.4, d: 3.4 },
  { id: 'hub_e_n', x: 7.5, z: -4.8, w: 1, d: 5.4 },
  { id: 'hub_e_s', x: 7.5, z: 4.8, w: 1, d: 5.4 },

  { id: 'south_w', x: -9.5, z: 16, w: 1, d: 17 },
  { id: 'south_e', x: 9.5, z: 16, w: 1, d: 17 },
  { id: 'south_far', x: 0, z: 24.5, w: 20, d: 1 },
  { id: 'arch_rack_1', x: -4.8, z: 14.5, w: 1.2, d: 6.5 },
  { id: 'arch_rack_2', x: 4.8, z: 14.5, w: 1.2, d: 6.5 },
  { id: 'arch_rack_3', x: 0, z: 19.5, w: 5.2, d: 1.2 },

  { id: 'hub_console', x: 0, z: 0, w: 2.2, d: 2.2 },
];

const INITIAL_OBJECTS = [
  // Гермодвери
  {
    id: 'door_west',
    type: 'door',
    label: 'Гермодверь: Западная лаборатория',
    x: -7.5,
    z: 0,
    rotY: Math.PI / 2,
    width: 2.8,
    state: { isOpen: false, locked: false },
  },
  {
    id: 'door_east',
    type: 'door',
    label: 'Гермодверь: Генераторный блок',
    x: 7.5,
    z: 0,
    rotY: Math.PI / 2,
    width: 2.8,
    state: { isOpen: false, locked: false },
  },
  {
    id: 'door_south',
    type: 'door',
    label: 'Гермодверь: Южный архив (нужна ключ-карта)',
    x: 0,
    z: 7.5,
    rotY: 0,
    width: 2.8,
    state: { isOpen: false, locked: true, requiresKeycard: true },
  },
  {
    id: 'door_airlock',
    type: 'door',
    label: 'Шлюз эвакуации «Альфа» (нужны 4 реле)',
    x: 0,
    z: -20.0,
    rotY: 0,
    width: 3.0,
    state: { isOpen: false, locked: true, requiresRelays: 4 },
  },

  // Ключ-карта доступа (координаты перезаписываются сервером случайно каждый матч)
  {
    id: 'keycard_archive',
    type: 'keycard',
    label: 'Ключ-карта доступа: Южный архив',
    x: -22.2,
    z: 6.2,
    rotY: 0,
    state: { pickedUp: false },
  },

  // Аварийные реле питания (4 штуки)
  {
    id: 'relay_west',
    type: 'relay',
    label: 'Реле питания №1 (Лаборатория)',
    x: -22.5,
    z: -5.5,
    rotY: 0,
    state: { active: false },
  },
  {
    id: 'relay_east_1',
    type: 'relay',
    label: 'Реле питания №2 (Турбина А)',
    x: 22.8,
    z: -5.5,
    rotY: Math.PI,
    state: { active: false },
  },
  {
    id: 'relay_east_2',
    type: 'relay',
    label: 'Реле питания №3 (Турбина Б)',
    x: 22.8,
    z: 5.5,
    rotY: Math.PI,
    state: { active: false },
  },
  {
    id: 'relay_south',
    type: 'relay',
    label: 'Реле питания №4 (Глубинный архив)',
    x: 0,
    z: 22.8,
    rotY: Math.PI,
    state: { active: false },
  },

  // Консоль эвакуации за шлюзом (требует 4-значный код!)
  {
    id: 'evac_terminal',
    type: 'evac_terminal',
    label: 'Терминал подъёмника (ввод кода)',
    x: 0,
    z: -25.4,
    rotY: 0,
    state: { activated: false },
  },

  // 4 архивные записки — каждая содержит ЛОР + 1 случайную цифру кода эвакуации
  {
    id: 'note_hub',
    type: 'note',
    label: 'Журнал дежурного [Код 1/4]',
    x: 0,
    z: -1.45,
    rotY: 0,
    state: {
      codeIndex: 0,
      title: 'Журнал дежурного — 14 октября',
      text: 'После запуска акустического резонатора экипаж начал слышать шаги в пустых отсеках. Дверь в Южный архив заблокирована — ключ-карта осталась в Западной лаборатории или Генераторном блоке.',
    },
  },
  {
    id: 'note_west',
    type: 'note',
    label: 'Протокол «Эхо» [Код 2/4]',
    x: -18.5,
    z: 6.2,
    rotY: 0,
    state: {
      codeIndex: 1,
      title: 'Протокол наблюдений №09-Б',
      text: 'Оно не преследует группу. Оно слушает. Когда человек отходит дальше 12 метров от напарников, радиоканал начинает подделывать голоса.',
    },
  },
  {
    id: 'note_east',
    type: 'note',
    label: 'Сводка энергоблока [Код 3/4]',
    x: 16.8,
    z: 0,
    rotY: 0,
    state: {
      codeIndex: 2,
      title: 'Сводка сменного энергетика',
      text: 'При сильном скачке напряжения одно из активных реле может выбить обратно — следите за показаниями на HUD и не выключайте рацию.',
    },
  },
  {
    id: 'note_south',
    type: 'note',
    label: 'Записка Маркова [Код 4/4]',
    x: -7.2,
    z: 21.0,
    rotY: 0,
    state: {
      codeIndex: 3,
      title: 'Последняя запись Маркова',
      text: 'Подъёмник в шлюзе «Альфа» защищён 4-значным кодом. Соберите все 4 фрагмента кода из журналов станции, поднимите 4 реле и введите код на северном терминале.',
    },
  },
];

// Поджатый радиус потолочных ламп — меньше просветов сквозь стены, глубже темнота
const LIGHT_FIXTURES = [
  { id: 'l_hub', x: 0, z: 0, color: 0xb7d4e6, intensity: 1.25, distance: 10.5 },
  { id: 'l_nc1', x: 0, z: -11, color: 0x9fc2d8, intensity: 0.95, distance: 8.5 },
  { id: 'l_nc2', x: 0, z: -17, color: 0x9fc2d8, intensity: 0.85, distance: 8.5 },
  { id: 'l_airlock', x: 0, z: -23.5, color: 0x86e0c2, intensity: 1.05, distance: 9.0 },
  { id: 'l_west1', x: -11.5, z: 0, color: 0xa2c7dd, intensity: 0.95, distance: 9.0 },
  { id: 'l_west2', x: -20, z: 0, color: 0x8fb4cc, intensity: 0.9, distance: 9.5 },
  { id: 'l_east1', x: 11.5, z: 0, color: 0xd8b88a, intensity: 0.95, distance: 9.0 },
  { id: 'l_east2', x: 19.5, z: 0, color: 0xd8a97a, intensity: 0.9, distance: 9.5 },
  { id: 'l_south1', x: 0, z: 12.5, color: 0x9ab7cc, intensity: 0.95, distance: 9.5 },
  { id: 'l_south2', x: 0, z: 21.5, color: 0x8ea9bd, intensity: 0.85, distance: 9.5 },
];

const ANOMALY_POINTS = [
  { x: 0, z: -15.5, zone: 'north_corridor' },
  { x: -19.5, z: -4.5, zone: 'west_wing' },
  { x: -19.5, z: 5.5, zone: 'west_wing' },
  { x: 17.5, z: -5.5, zone: 'east_wing' },
  { x: 17.5, z: 5.5, zone: 'east_wing' },
  { x: -7.0, z: 18.0, zone: 'south_archive' },
  { x: 7.0, z: 18.0, zone: 'south_archive' },
];

function getZoneAt(x, z) {
  for (const zone of ZONES) {
    if (x >= zone.minX && x <= zone.maxX && z >= zone.minZ && z <= zone.maxZ) {
      return zone;
    }
  }
  return { id: 'unknown', name: 'Технический переход' };
}

/**
 * 2D-проверка пересечения отрезка (x1,z1)->(x2,z2) со стенами (Slab method).
 * Возвращает true, если между точками НЕТ стен (прямая видимость чистая).
 */
function hasLineOfSight(x1, z1, x2, z2, closedDoors = [], ignoreDoorId = null) {
  const boxes = [...WALLS];
  for (const door of closedDoors) {
    if (door.id === ignoreDoorId) continue;
    const isHorizontal = Math.abs(Math.cos(door.rotY || 0)) > 0.5;
    boxes.push({
      x: door.x,
      z: door.z,
      w: isHorizontal ? door.width : 0.45,
      d: isHorizontal ? 0.45 : door.width,
    });
  }

  // Слегка отступаем от конечной точки, чтобы объект, висящий на стене, не блокировал сам себя
  const dx = x2 - x1;
  const dz = z2 - z1;
  const len = Math.hypot(dx, dz);
  if (len < 0.05) return true;
  const shrink = Math.min(0.28, len * 0.25);
  const ex = x2 - (dx / len) * shrink;
  const ez = z2 - (dz / len) * shrink;

  for (const b of boxes) {
    if (segmentIntersectsAABB(x1, z1, ex, ez, b.x - b.w / 2, b.x + b.w / 2, b.z - b.d / 2, b.z + b.d / 2)) {
      return false;
    }
  }
  return true;
}

function segmentIntersectsAABB(x1, z1, x2, z2, minX, maxX, minZ, maxZ) {
  let t0 = 0;
  let t1 = 1;
  const dx = x2 - x1;
  const dz = z2 - z1;

  const clip = (p, q) => {
    if (Math.abs(p) < 1e-8) return q >= 0;
    const r = q / p;
    if (p < 0) {
      if (r > t1) return false;
      if (r > t0) t0 = r;
    } else {
      if (r < t0) return false;
      if (r < t1) t1 = r;
    }
    return true;
  };

  return (
    clip(-dx, x1 - minX) &&
    clip(dx, maxX - x1) &&
    clip(-dz, z1 - minZ) &&
    clip(dz, maxZ - z1)
  );
}

/**
 * Находит свободную видимую точку в коридоре перед игроком (для спавна личного фантома без попадания в стены).
 */
function findVisibleCorridorPoint(px, pz, yaw, closedDoors = []) {
  const dirX = -Math.sin(yaw);
  const dirZ = -Math.cos(yaw);
  for (const dist of [7.5, 6.2, 5.0, 4.0]) {
    const cx = px + dirX * dist;
    const cz = pz + dirZ * dist;
    const resolved = resolveMapCollision(cx, cz, closedDoors);
    const shifted = Math.hypot(resolved.x - cx, resolved.z - cz);
    if (shifted < 0.15 && hasLineOfSight(px, pz, cx, cz, closedDoors)) {
      return { x: Number(cx.toFixed(2)), z: Number(cz.toFixed(2)) };
    }
  }
  return null;
}

function resolveMapCollision(x, z, closedDoors = []) {
  const r = MAP_CONFIG.playerRadius;
  let nx = Math.max(MAP_CONFIG.worldBounds.minX + r, Math.min(MAP_CONFIG.worldBounds.maxX - r, x));
  let nz = Math.max(MAP_CONFIG.worldBounds.minZ + r, Math.min(MAP_CONFIG.worldBounds.maxZ - r, z));

  const boxes = [...WALLS];
  for (const door of closedDoors) {
    const isHorizontal = Math.abs(Math.cos(door.rotY || 0)) > 0.5;
    boxes.push({
      x: door.x,
      z: door.z,
      w: isHorizontal ? door.width : 0.6,
      d: isHorizontal ? 0.6 : door.width,
    });
  }

  for (let pass = 0; pass < 2; pass++) {
    for (const b of boxes) {
      const halfW = b.w / 2;
      const halfD = b.d / 2;
      const closestX = Math.max(b.x - halfW, Math.min(nx, b.x + halfW));
      const closestZ = Math.max(b.z - halfD, Math.min(nz, b.z + halfD));
      const dx = nx - closestX;
      const dz = nz - closestZ;
      const distSq = dx * dx + dz * dz;

      if (distSq < r * r) {
        const dist = Math.sqrt(distSq);
        if (dist > 0.0001) {
          const push = r - dist;
          nx += (dx / dist) * push;
          nz += (dz / dist) * push;
        } else {
          const penX = halfW - Math.abs(nx - b.x);
          const penZ = halfD - Math.abs(nz - b.z);
          if (penX < penZ) {
            nx += nx >= b.x ? penX + r : -(penX + r);
          } else {
            nz += nz >= b.z ? penZ + r : -(penZ + r);
          }
        }
      }
    }
  }

  return { x: nx, z: nz };
}

// === SHARED: characterCatalog.js ===
/**
 * Расширяемый каталог персонажей и экипировки.
 * Чтобы добавить нового персонажа или косметику, достаточно добавить запись сюда.
 */

const CHARACTER_SKINS = [
  {
    id: 'engineer',
    name: 'Инженер связи',
    suitColor: 0x3b6e8c,
    accentColor: 0xf0a830,
    visorColor: 0x7ce8ff,
    flashlightColor: 0xfff3d6,
    badgeHex: '#54b7e8',
  },
  {
    id: 'acoustic',
    name: 'Акустик-исследователь',
    suitColor: 0x456b52,
    accentColor: 0x8be29a,
    visorColor: 0xa6ffcb,
    flashlightColor: 0xe8fff2,
    badgeHex: '#63d48c',
  },
  {
    id: 'medic',
    name: 'Полевой медик',
    suitColor: 0x8a4242,
    accentColor: 0xf4ece6,
    visorColor: 0xffd0b0,
    flashlightColor: 0xfff0e0,
    badgeHex: '#e87878',
  },
  {
    id: 'archivist',
    name: 'Архивариус',
    suitColor: 0x62507d,
    accentColor: 0xd4b8ff,
    visorColor: 0xc2d9ff,
    flashlightColor: 0xe6eaff,
    badgeHex: '#b392eb',
  },
];

function getSkinById(id) {
  return CHARACTER_SKINS.find((s) => s.id === id) || CHARACTER_SKINS[0];
}

// === SERVER: config.js ===
/**
 * Конфигурация сервера (читает переменные окружения для Render/Railway/Docker/локального запуска).
 */

const SERVER_CONFIG = {
  host: process.env.HOST || '0.0.0.0',
  port: Number.parseInt(process.env.PORT || '3000', 10),
  maxRooms: Number.parseInt(process.env.MAX_ROOMS || '50', 10),
  tickRateHz: Number.parseInt(process.env.TICK_RATE || '15', 10),
  heartbeatIntervalMs: 15_000,
  roomIdleCleanupMs: 120_000,
};

// === SERVER: wsServer.js ===
/**
 * Лёгкий RFC 6455 WebSocket-сервер на встроенных модулях Node.js (без внешних npm-зависимостей).
 * Поддерживает: handshake, текстовые фреймы (до 64 КБ), фрагментацию/маскирование,
 * ping/pong heartbeat, корректное закрытие и защиту от повреждённых пакетов и обрывов EPIPE.
 */


const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class WebSocketClient extends EventEmitter {
  constructor(socket, request) {
    super();
    this.socket = socket;
    this.request = request;
    this.readyState = 1; // OPEN
    this.isAlive = true;
    this._buffer = Buffer.alloc(0);

    socket.setNoDelay(true);
    socket.on('data', (chunk) => this._onData(chunk));
    socket.on('close', () => this._handleClose(1006, 'Connection closed'));
    socket.on('error', (err) => {
      if (this.listenerCount('error') > 0) {
        this.emit('error', err);
      }
      this._handleClose(1006, 'Socket error');
    });
  }

  send(data) {
    if (this.readyState !== 1 || !this.socket.writable || this.socket.destroyed) return false;
    const payload = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
    const frame = encodeFrame(0x1, payload);
    try {
      this.socket.write(frame);
      return true;
    } catch {
      this._handleClose(1006, 'Write failed');
      return false;
    }
  }

  sendJSON(obj) {
    return this.send(JSON.stringify(obj));
  }

  ping() {
    if (this.readyState !== 1 || !this.socket.writable || this.socket.destroyed) return;
    try {
      this.socket.write(encodeFrame(0x9, Buffer.alloc(0)));
    } catch {
      this._handleClose(1006, 'Ping failed');
    }
  }

  close(code = 1000, reason = '') {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    const reasonBuf = Buffer.from(reason, 'utf8');
    const payload = Buffer.alloc(2 + reasonBuf.length);
    payload.writeUInt16BE(code, 0);
    reasonBuf.copy(payload, 2);
    try {
      if (this.socket.writable && !this.socket.destroyed) {
        this.socket.write(encodeFrame(0x8, payload));
        this.socket.end();
      }
    } catch {
      // ignore
    }
    this._handleClose(code, reason);
  }

  _handleClose(code, reason) {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit('close', code, reason);
  }

  _onData(chunk) {
    this._buffer = Buffer.concat([this._buffer, chunk]);

    // Защита от переполнения памяти
    if (this._buffer.length > 256 * 1024) {
      this.close(1009, 'Message too big');
      return;
    }

    while (this._buffer.length >= 2) {
      const firstByte = this._buffer[0];
      const secondByte = this._buffer[1];
      const opcode = firstByte & 0x0f;
      const masked = (secondByte & 0x80) !== 0;
      let payloadLen = secondByte & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (this._buffer.length < 4) return;
        payloadLen = this._buffer.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (this._buffer.length < 10) return;
        const high = this._buffer.readUInt32BE(2);
        const low = this._buffer.readUInt32BE(6);
        if (high !== 0 || low > 65536) {
          this.close(1009, 'Frame too large');
          return;
        }
        payloadLen = low;
        offset = 10;
      }

      const maskLen = masked ? 4 : 0;
      const totalLen = offset + maskLen + payloadLen;
      if (this._buffer.length < totalLen) return;

      let maskKey = null;
      if (masked) {
        maskKey = this._buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      const payload = Buffer.from(this._buffer.subarray(offset, offset + payloadLen));
      this._buffer = this._buffer.subarray(totalLen);

      if (masked && maskKey) {
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= maskKey[i & 3];
        }
      }

      if (opcode === 0x8) {
        const code = payload.length >= 2 ? payload.readUInt16BE(0) : 1000;
        const reason = payload.length > 2 ? payload.subarray(2).toString('utf8') : '';
        this.close(code, reason);
        return;
      } else if (opcode === 0x9) {
        this.isAlive = true;
        try {
          if (this.socket.writable && !this.socket.destroyed) {
            this.socket.write(encodeFrame(0xa, payload));
          }
        } catch {
          this._handleClose(1006, 'Pong failed');
        }
      } else if (opcode === 0xa) {
        this.isAlive = true;
      } else if (opcode === 0x1) {
        this.isAlive = true;
        this.emit('message', payload.toString('utf8'));
      }
    }
  }
}

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeUInt32BE(0, 2);
    header.writeUInt32BE(len, 6);
  }
  return Buffer.concat([header, payload]);
}

class WebSocketServer extends EventEmitter {
  constructor(httpServer, { heartbeatIntervalMs = 15_000 } = {}) {
    super();
    this.clients = new Set();

    httpServer.on('upgrade', (req, socket) => {
      const upgradeHeader = (req.headers.upgrade || '').toLowerCase();
      const wsKey = req.headers['sec-websocket-key'];
      if (upgradeHeader !== 'websocket' || !wsKey) {
        socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
        socket.destroy();
        return;
      }

      const acceptKey = crypto
        .createHash('sha1')
        .update(wsKey + WS_GUID)
        .digest('base64');

      const headers = [
        'HTTP/1.1 101 Switching Protocols',
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Accept: ${acceptKey}`,
        '\r\n',
      ];
      socket.write(headers.join('\r\n'));

      const client = new WebSocketClient(socket, req);
      this.clients.add(client);
      client.on('close', () => this.clients.delete(client));
      this.emit('connection', client, req);
    });

    this._heartbeatTimer = setInterval(() => {
      for (const client of this.clients) {
        if (!client.isAlive) {
          client.close(1001, 'Heartbeat timeout');
          continue;
        }
        client.isAlive = false;
        client.ping();
      }
    }, heartbeatIntervalMs);
    this._heartbeatTimer.unref?.();
  }

  close() {
    clearInterval(this._heartbeatTimer);
    for (const client of this.clients) {
      client.close(1001, 'Server shutting down');
    }
    this.clients.clear();
  }
}

// === SERVER: eventDefinitions.js ===
/**
 * Расширяемый реестр хоррор-событий (общие, индивидуальные и динамические).
 * Личный фантом теперь проверяет геометрию коридора и не появляется внутри стен.
 */


const HORROR_EVENTS = [
  // ================= ОБЩИЕ СОБЫТИЯ =================
  {
    id: 'global_power_flicker',
    scope: EVENT_SCOPE.GLOBAL,
    cooldownMs: 30_000,
    minPlayers: 1,
    weight: 3,
    canTrigger: (ctx) => ctx.tension >= 0.15,
    buildPayload: () => ({
      effect: 'power_flicker',
      durationMs: 4200,
      sound: 'power_drop',
      subtitle: 'Напряжение в сети станции резко просело...',
    }),
  },
  {
    id: 'global_emergency_red',
    scope: EVENT_SCOPE.GLOBAL,
    cooldownMs: 52_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: (ctx) => ctx.activatedRelays >= 1,
    buildPayload: () => ({
      effect: 'emergency_red',
      durationMs: 11_000,
      sound: 'klaxon_distant',
      subtitle: 'Аварийное освещение перешло в красный режим.',
    }),
  },
  {
    id: 'global_bulkhead_slam',
    scope: EVENT_SCOPE.GLOBAL,
    cooldownMs: 44_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: (ctx) => ctx.openUnlockableDoors.length > 0,
    executeServer: (ctx) => {
      const door = ctx.openUnlockableDoors[Math.floor(Math.random() * ctx.openUnlockableDoors.length)];
      if (door) {
        door.state.isOpen = false;
        ctx.broadcastObject(door);
        return { doorId: door.id, doorLabel: door.label, x: door.x, z: door.z };
      }
      return null;
    },
    buildPayload: (ctx, serverResult) => ({
      effect: 'bulkhead_slam',
      doorId: serverResult?.doorId || null,
      x: serverResult?.x ?? 0,
      z: serverResult?.z ?? 0,
      durationMs: 3000,
      sound: 'metal_slam',
      subtitle: serverResult
        ? `Вдалеке с грохотом захлопнулась ${serverResult.doorLabel.toLowerCase()}.`
        : 'Где-то в глубине сектора захлопнулась переборка.',
    }),
  },
  {
    id: 'global_acoustic_hum',
    scope: EVENT_SCOPE.GLOBAL,
    cooldownMs: 38_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: () => true,
    buildPayload: () => ({
      effect: 'acoustic_hum',
      durationMs: 7500,
      sound: 'sub_hum',
      subtitle: 'По стенам прошла низкая вибрация резонатора.',
    }),
  },
  {
    id: 'global_pa_broadcast',
    scope: EVENT_SCOPE.GLOBAL,
    cooldownMs: 60_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: (ctx) => ctx.elapsedSec > 25,
    buildPayload: (ctx) => {
      const lines = [
        '«...повторяю: подсчёт персонала не сходится. На уровне на одного человека больше...»',
        '«...акустическая камера не заперта. Не оборачивайтесь на голос из рации...»',
        '«...реле №4 отзывается из пустого архива. Кто там находится?..»',
      ];
      return {
        effect: 'pa_broadcast',
        durationMs: 6500,
        sound: 'radio_static',
        subtitle: lines[ctx.eventCounter % lines.length],
      };
    },
  },

  // ================= ИНДИВИДУАЛЬНЫЕ СОБЫТИЯ =================
  {
    id: 'solo_phantom_footsteps',
    scope: EVENT_SCOPE.INDIVIDUAL,
    cooldownMs: 26_000,
    minPlayers: 1,
    weight: 3,
    canTrigger: () => true,
    selectTarget: (ctx) => ctx.pickMostIsolatedOrRandom(),
    buildPayload: (ctx, _res, target) => ({
      effect: 'phantom_footsteps',
      x: target.x - Math.sin(target.yaw) * 3.5,
      z: target.z - Math.cos(target.yaw) * 3.5,
      durationMs: 4500,
      sound: 'steps_behind',
      subtitle: 'Прямо за вашей спиной прозвучали тяжёлые шаги — но там никого нет.',
      composureDelta: -10,
    }),
  },
  {
    id: 'solo_personal_silhouette',
    scope: EVENT_SCOPE.INDIVIDUAL,
    cooldownMs: 40_000,
    minPlayers: 1,
    weight: 3,
    canTrigger: (ctx) => ctx.elapsedSec > 18,
    selectTarget: (ctx) => ctx.pickMostIsolatedOrRandom(),
    buildPayload: (ctx, _res, target) => {
      const closedDoors = ctx.room.getClosedDoors();
      const pt = findVisibleCorridorPoint(target.x, target.z, target.yaw, closedDoors);
      if (!pt) {
        // Если игрок смотрит вплотную в стену — заменяем визуальный фантом на шёпот рядом
        return {
          effect: 'phantom_footsteps',
          durationMs: 4000,
          sound: 'whisper_close',
          subtitle: 'У самого уха кто-то тихо выдохнул... и стих.',
          composureDelta: -9,
        };
      }
      return {
        effect: 'personal_silhouette',
        x: pt.x,
        z: pt.z,
        durationMs: 4800,
        sound: 'whisper_close',
        subtitle: 'В конце коридора на мгновение замер высокий силуэт. Остальные его не видят.',
        composureDelta: -14,
      };
    },
  },
  {
    id: 'solo_false_radio',
    scope: EVENT_SCOPE.INDIVIDUAL,
    cooldownMs: 36_000,
    minPlayers: 2,
    weight: 3,
    canTrigger: (ctx) => ctx.activePlayers.length >= 2,
    selectTarget: (ctx) => ctx.pickMostIsolatedOrRandom(),
    buildPayload: (ctx, _res, target) => {
      const others = ctx.activePlayers.filter((p) => p.id !== target.id);
      const fakeSender = others[Math.floor(Math.random() * others.length)];
      const phrases = [
        'Ты почему стоишь ко мне спиной?',
        'Не заходи туда, я видел, как там погас свет.',
        'Подожди... а кто только что прошёл мимо тебя?',
      ];
      return {
        effect: 'false_radio',
        fakeSenderName: fakeSender ? fakeSender.name : 'Неизвестный',
        fakeMessage: phrases[Math.floor(Math.random() * phrases.length)],
        durationMs: 5500,
        sound: 'radio_glitch',
        subtitle: 'В вашей гарнитуре прорвалась странная передача...',
        composureDelta: -8,
      };
    },
  },
  {
    id: 'solo_flashlight_die',
    scope: EVENT_SCOPE.INDIVIDUAL,
    cooldownMs: 42_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: (ctx) => ctx.tension >= 0.25,
    selectTarget: (ctx) => ctx.pickMostIsolatedOrRandom(),
    buildPayload: () => ({
      effect: 'flashlight_glitch',
      durationMs: 5200,
      sound: 'bulb_buzz',
      subtitle: 'Ваш фонарь начал мерцать и затухать сам по себе.',
      composureDelta: -6,
    }),
  },

  // ================= ДИНАМИЧЕСКИЕ СОБЫТИЯ =================
  {
    id: 'dynamic_isolation_dread',
    scope: EVENT_SCOPE.DYNAMIC,
    targetMode: 'individual',
    cooldownMs: 26_000,
    minPlayers: 2,
    weight: 4,
    canTrigger: (ctx) => ctx.isolatedPlayers.length > 0,
    selectTarget: (ctx) => ctx.isolatedPlayers[Math.floor(Math.random() * ctx.isolatedPlayers.length)],
    buildPayload: () => ({
      effect: 'isolation_dread',
      durationMs: 6500,
      sound: 'heartbeat_heavy',
      subtitle: 'Вы отошли слишком далеко от группы. Тишина давит на уши.',
      composureDelta: -12,
    }),
  },
  {
    id: 'dynamic_relay_overload',
    scope: EVENT_SCOPE.DYNAMIC,
    targetMode: 'global',
    cooldownMs: 75_000,
    minPlayers: 1,
    weight: 2,
    canTrigger: (ctx) => ctx.activatedRelays >= 2 && ctx.activatedRelays < 4 && ctx.tension >= 0.45,
    executeServer: (ctx) => {
      const activeRelays = [];
      for (const obj of ctx.room.objects.values()) {
        if (obj.type === 'relay' && obj.state.active) activeRelays.push(obj);
      }
      if (activeRelays.length === 0) return null;
      const tripped = activeRelays[Math.floor(Math.random() * activeRelays.length)];
      tripped.state.active = false;
      ctx.broadcastObject(tripped);
      return { relayLabel: tripped.label };
    },
    buildPayload: (_ctx, res) => ({
      effect: 'power_flicker',
      durationMs: 4800,
      sound: 'power_drop',
      subtitle: res
        ? `Перегрузка сети! Выбило ${res.relayLabel.toLowerCase()} — нужно включить снова.`
        : 'Скачок напряжения едва не обесточил сектор.',
    }),
  },
  {
    id: 'dynamic_relay_surge',
    scope: EVENT_SCOPE.DYNAMIC,
    targetMode: 'global',
    cooldownMs: 8_000,
    minPlayers: 1,
    weight: 0,
    triggerOnAction: 'relay_activated',
    buildPayload: (_ctx, actionData) => ({
      effect: 'relay_surge',
      relayCount: actionData.relayCount,
      requiredRelays: actionData.requiredRelays,
      durationMs: 5000,
      sound: actionData.relayCount >= actionData.requiredRelays ? 'airlock_unlock' : 'power_surge',
      subtitle:
        actionData.relayCount >= actionData.requiredRelays
          ? 'Все 4 реле активны! Шлюз «Альфа» открыт — введите 4-значный код на северном терминале.'
          : `Реле подключено (${actionData.relayCount}/${actionData.requiredRelays}). Что-то в тёмных коридорах откликнулось на скачок тока.`,
    }),
  },
];

// === SERVER: EventEngine.js ===
/**
 * Серверный движок игровых событий.
 * Управляет общими, индивидуальными и динамическими событиями на основе состояния комнаты.
 */


class EventEngine {
  constructor(room) {
    this.room = room;
    this.events = [...HORROR_EVENTS];
    this.cooldowns = new Map();
    this.nextCheckAt = 0;
    this.eventCounter = 0;
    this.tension = 0.15;
  }

  /**
   * Позволяет регистрировать новые события во время выполнения без правки ядра.
   */
  registerEvent(definition) {
    this.events.push(definition);
  }

  reset() {
    this.cooldowns.clear();
    this.eventCounter = 0;
    this.tension = 0.15;
    this.nextCheckAt = Date.now() + 12_000;
  }

  /**
   * Вызывается по тику сервера, пока сессия активна.
   */
  tick(now, elapsedSec) {
    // Плавный рост напряжения со временем и по мере активации реле
    const activatedRelays = this.room.getActivatedRelayCount();
    this.tension = Math.min(1.0, 0.15 + elapsedSec / 360 + activatedRelays * 0.16);

    if (now < this.nextCheckAt) return;

    const ctx = this._buildContext(elapsedSec, activatedRelays);
    if (ctx.activePlayers.length === 0) return;

    const candidates = this.events.filter((ev) => {
      if (!ev.weight || ev.weight <= 0) return false;
      if (ctx.activePlayers.length < (ev.minPlayers || 1)) return false;
      const last = this.cooldowns.get(ev.id) || 0;
      if (now - last < (ev.cooldownMs || 20_000)) return false;
      return ev.canTrigger ? ev.canTrigger(ctx) : true;
    });

    if (candidates.length > 0) {
      const chosen = weightedPick(candidates);
      this._dispatchEvent(chosen, ctx, now);
    }

    // Частота событий адаптируется к напряжению (11–20 сек, чтобы оставлять паузы тишины)
    const minWait = Math.max(9_000, 18_000 - this.tension * 7_000);
    const maxWait = minWait + 8_000;
    this.nextCheckAt = now + minWait + Math.random() * (maxWait - minWait);
  }

  /**
   * Запуск динамического события по игровому действию (например, активация реле).
   */
  triggerAction(actionName, actionData = {}) {
    const now = Date.now();
    const matching = this.events.filter((ev) => ev.triggerOnAction === actionName);
    if (matching.length === 0) return;

    const ctx = this._buildContext(this.room.getElapsedSeconds(), this.room.getActivatedRelayCount());
    for (const ev of matching) {
      this._dispatchEvent(ev, ctx, now, actionData);
    }
  }

  _dispatchEvent(ev, ctx, now, actionData = null) {
    this.cooldowns.set(ev.id, now);
    this.eventCounter++;

    const serverResult = ev.executeServer ? ev.executeServer(ctx, actionData) : actionData;
    const isIndividual =
      ev.scope === EVENT_SCOPE.INDIVIDUAL ||
      (ev.scope === EVENT_SCOPE.DYNAMIC && ev.targetMode === 'individual');

    if (isIndividual) {
      const target = ev.selectTarget ? ev.selectTarget(ctx) : ctx.pickMostIsolatedOrRandom();
      if (!target) return;

      const payload = ev.buildPayload(ctx, serverResult, target);
      if (typeof payload.composureDelta === 'number') {
        target.composure = Math.max(10, Math.min(100, target.composure + payload.composureDelta));
      }

      this.room.sendToPlayer(target.id, {
        type: MSG.HORROR_EVENT,
        eventId: ev.id,
        scope: ev.scope,
        timestamp: now,
        payload,
      });
    } else {
      const payload = ev.buildPayload(ctx, serverResult, null);
      this.room.broadcast({
        type: MSG.HORROR_EVENT,
        eventId: ev.id,
        scope: ev.scope,
        timestamp: now,
        payload,
      });
    }
  }

  _buildContext(elapsedSec, activatedRelays) {
    const activePlayers = this.room.getConnectedPlayers();
    const isolatedPlayers = [];

    // Ищем игроков, которые отошли дальше 13 м от ближайшего напарника
    if (activePlayers.length >= 2) {
      for (const p of activePlayers) {
        let minDist = Infinity;
        for (const other of activePlayers) {
          if (other.id === p.id) continue;
          const d = Math.hypot(p.x - other.x, p.z - other.z);
          if (d < minDist) minDist = d;
        }
        if (minDist > 13.0) {
          isolatedPlayers.push(p);
        }
      }
    }

    const openUnlockableDoors = [];
    for (const obj of this.room.objects.values()) {
      if (obj.type === 'door' && obj.state.isOpen && !obj.state.requiresRelays) {
        openUnlockableDoors.push(obj);
      }
    }

    return {
      room: this.room,
      activePlayers,
      isolatedPlayers,
      openUnlockableDoors,
      activatedRelays,
      elapsedSec,
      tension: this.tension,
      eventCounter: this.eventCounter,
      broadcastObject: (obj) => this.room.broadcastObjectUpdate(obj),
      pickMostIsolatedOrRandom: () => {
        if (isolatedPlayers.length > 0) {
          return isolatedPlayers[Math.floor(Math.random() * isolatedPlayers.length)];
        }
        return activePlayers[Math.floor(Math.random() * activePlayers.length)] || null;
      },
    };
  }
}

function weightedPick(items) {
  const total = items.reduce((acc, item) => acc + (item.weight || 1), 0);
  let r = Math.random() * total;
  for (const item of items) {
    r -= item.weight || 1;
    if (r <= 0) return item;
  }
  return items[items.length - 1];
}

// === SERVER: AnomalyDirector.js ===
/**
 * Атмосферная сущность «Эхо-Силуэт» (серверный контроллер).
 * Это НЕ аркадный преследователь: сущность появляется на границе видимости
 * в тёмных коридорах, наблюдает за группой или одиночкой и исчезает при сближении.
 */


class AnomalyDirector {
  constructor() {
    this.reset();
  }

  reset() {
    this.active = false;
    this.x = 0;
    this.z = -15.5;
    this.yaw = 0;
    this.opacity = 0;
    this.visibleUntil = 0;
    this.nextSpawnAt = Date.now() + 24_000;
  }

  tick(now, players, tension) {
    if (players.length === 0) {
      this.active = false;
      return;
    }

    if (this.active) {
      // Если кто-то подошёл ближе 5.5м или время вышло — силуэт растворяется
      let nearest = Infinity;
      let nearestPlayer = null;
      for (const p of players) {
        const d = Math.hypot(p.x - this.x, p.z - this.z);
        if (d < nearest) {
          nearest = d;
          nearestPlayer = p;
        }
      }

      if (nearestPlayer) {
        this.yaw = Math.atan2(nearestPlayer.x - this.x, nearestPlayer.z - this.z);
      }

      if (nearest < 5.5 || now >= this.visibleUntil) {
        this.active = false;
        this.opacity = 0;
        this.nextSpawnAt = now + 22_000 + Math.random() * 18_000;
      } else {
        this.opacity = Math.min(0.85, 0.35 + tension * 0.5);
      }
      return;
    }

    if (now < this.nextSpawnAt) return;

    // Выбираем точку в диапазоне 9..22 м от ближайшего игрока (на границе света фонаря)
    const candidates = ANOMALY_POINTS.filter((pt) => {
      let minDist = Infinity;
      for (const p of players) {
        const d = Math.hypot(p.x - pt.x, p.z - pt.z);
        if (d < minDist) minDist = d;
      }
      return minDist >= 9 && minDist <= 22;
    });

    if (candidates.length === 0) {
      this.nextSpawnAt = now + 8_000;
      return;
    }

    const chosen = candidates[Math.floor(Math.random() * candidates.length)];
    this.active = true;
    this.x = chosen.x;
    this.z = chosen.z;
    this.opacity = 0.55;
    this.visibleUntil = now + 9_000 + Math.random() * 7_000;
  }

  serialize() {
    return {
      active: this.active,
      x: Number(this.x.toFixed(2)),
      z: Number(this.z.toFixed(2)),
      yaw: Number(this.yaw.toFixed(2)),
      opacity: Number(this.opacity.toFixed(2)),
    };
  }
}

// === SERVER: GameRoom.js ===
/**
 * Авторитетная игровая комната и сессия (v1.2).
 * Добавлено: синхронизация двух турбин (relay_east_1 + relay_east_2) в окне 20 секунд при игре 2+ игроков.
 */


class GameRoom {
  constructor({ id, name, isPrivate, inviteCode, maxPlayers, hostPlayerId, tickRateHz = 15 }) {
    this.id = id;
    this.name = name;
    this.isPrivate = Boolean(isPrivate);
    this.inviteCode = inviteCode;
    this.maxPlayers = Math.max(2, Math.min(LIMITS.MAX_PLAYERS_CAP, maxPlayers || LIMITS.MAX_PLAYERS_DEFAULT));
    this.hostPlayerId = hostPlayerId;
    this.status = ROOM_STATUS.LOBBY;
    this.createdAt = Date.now();
    this.startedAt = null;
    this.lastActivityAt = Date.now();

    this.players = new Map();
    this.objects = new Map();
    this.chatHistory = [];
    this.hasArchiveKeycard = false;
    this.evacCode = '0000';
    this._turbineSyncTimer = null;

    this.eventEngine = new EventEngine(this);
    this.anomalyDirector = new AnomalyDirector();

    this._tickInterval = null;
    this._tickMs = Math.round(1000 / Math.max(5, Math.min(30, tickRateHz)));
    this._resetWorldState();
  }

  _resetWorldState() {
    clearTimeout(this._turbineSyncTimer);
    this._turbineSyncTimer = null;
    this.objects.clear();
    this.hasArchiveKeycard = false;

    const digits = [
      crypto.randomInt(1, 10),
      crypto.randomInt(0, 10),
      crypto.randomInt(0, 10),
      crypto.randomInt(1, 10),
    ];
    this.evacCode = digits.join('');
    const keySpawn = KEYCARD_SPAWNS[crypto.randomInt(0, KEYCARD_SPAWNS.length)];

    for (const template of INITIAL_OBJECTS) {
      const copy = {
        ...template,
        state: JSON.parse(JSON.stringify(template.state)),
      };
      if (copy.id === 'keycard_archive') {
        copy.x = keySpawn.x;
        copy.z = keySpawn.z;
      }
      if (copy.type === 'note' && typeof copy.state.codeIndex === 'number') {
        const idx = copy.state.codeIndex;
        const digit = digits[idx];
        copy.state.codeDigit = String(digit);
        copy.state.text = `${copy.state.text}\n\n[ФРАГМЕНТ КОДА ЭВАКУАЦИИ ${idx + 1}/4]: цифра «${digit}»`;
      }
      this.objects.set(copy.id, copy);
    }

    this.eventEngine.reset();
    this.anomalyDirector.reset();
  }

  addPlayer(ws, { name, skinId }) {
    if (this.players.size >= this.maxPlayers) {
      return { error: 'Комната уже заполнена.' };
    }
    if (this.status === ROOM_STATUS.PLAYING) {
      return { error: 'Игровая сессия уже идёт. Попросите хоста создать новое лобби или дождитесь завершения.' };
    }

    const id = 'p_' + crypto.randomBytes(5).toString('hex');
    const reconnectToken = crypto.randomBytes(12).toString('hex');
    const spawn = SPAWN_POINTS[this.players.size % SPAWN_POINTS.length];
    const cleanName = sanitizeText(name, LIMITS.MAX_NAME_LENGTH) || `Сотрудник-${this.players.size + 1}`;
    const skin = getSkinById(skinId);

    const player = {
      id,
      reconnectToken,
      ws,
      connected: true,
      disconnectedAt: null,
      name: cleanName,
      skinId: skin.id,
      ready: false,
      x: spawn.x,
      y: 0,
      z: spawn.z,
      yaw: spawn.yaw,
      pitch: 0,
      speed: 0,
      flashlight: true,
      crouching: false,
      sprinting: false,
      composure: 100,
      animState: 'idle',
      lastStateAt: Date.now(),
      zoneName: getZoneAt(spawn.x, spawn.z).name,
    };

    this.players.set(id, player);
    if (!this.hostPlayerId || !this.players.has(this.hostPlayerId)) {
      this.hostPlayerId = id;
    }

    this.lastActivityAt = Date.now();
    this.broadcastLobbyState();
    this.broadcastSystemNotice(`${player.name} присоединился к комнате.`);
    return { player };
  }

  reconnectPlayer(ws, playerId, reconnectToken) {
    const player = this.players.get(playerId);
    if (!player || player.reconnectToken !== reconnectToken) {
      return { error: 'Сессия не найдена или истёк токен переподключения.' };
    }

    if (player.ws && player.ws !== ws && player.ws.readyState === 1) {
      player.ws.close(4000, 'Replaced by new connection');
    }

    player.ws = ws;
    player.connected = true;
    player.disconnectedAt = null;
    player.lastStateAt = Date.now();
    this.lastActivityAt = Date.now();

    this.broadcastSystemNotice(`${player.name} восстановил соединение.`);
    if (this.status === ROOM_STATUS.LOBBY) {
      this.broadcastLobbyState();
    } else if (this.status === ROOM_STATUS.PLAYING) {
      this.sendToPlayer(player.id, this._buildGameStartPacket());
    }
    return { player };
  }

  handleDisconnect(playerId, graceful = false) {
    const player = this.players.get(playerId);
    if (!player) return;

    if (graceful || this.status === ROOM_STATUS.LOBBY) {
      this.removePlayer(playerId, graceful ? 'покинул комнату' : 'отключился');
      return;
    }

    player.connected = false;
    player.ws = null;
    player.disconnectedAt = Date.now();
    this.broadcastSystemNotice(`${player.name} потерял связь. Ожидание переподключения (${Math.round(LIMITS.RECONNECT_GRACE_MS / 1000)} с)...`);
  }

  removePlayer(playerId, reasonText = 'покинул комнату') {
    const player = this.players.get(playerId);
    if (!player) return;

    this.players.delete(playerId);
    this.lastActivityAt = Date.now();

    if (this.hostPlayerId === playerId) {
      const next = this.getConnectedPlayers()[0] || [...this.players.values()][0] || null;
      this.hostPlayerId = next ? next.id : null;
      if (next) {
        this.broadcastSystemNotice(`Права хоста переданы игроку ${next.name}.`);
      }
    }

    this.broadcastSystemNotice(`${player.name} ${reasonText}.`);

    if (this.players.size === 0) {
      this.stopLoop();
      return;
    }

    if (this.status === ROOM_STATUS.LOBBY) {
      this.broadcastLobbyState();
    }
  }

  updatePlayerProfile(playerId, { name, skinId }) {
    const player = this.players.get(playerId);
    if (!player) return;
    if (name) {
      const clean = sanitizeText(name, LIMITS.MAX_NAME_LENGTH);
      if (clean) player.name = clean;
    }
    if (skinId) {
      player.skinId = getSkinById(skinId).id;
    }
    this.broadcastLobbyState();
  }

  setPlayerReady(playerId, ready) {
    const player = this.players.get(playerId);
    if (!player || this.status !== ROOM_STATUS.LOBBY) return;
    player.ready = Boolean(ready);
    this.broadcastLobbyState();
  }

  startGame(requesterId) {
    if (requesterId !== this.hostPlayerId) {
      return { error: 'Только хост комнаты может запустить экспедицию.' };
    }
    if (this.status !== ROOM_STATUS.LOBBY) {
      return { error: 'Игра уже запущена.' };
    }
    const connected = this.getConnectedPlayers();
    if (connected.length < LIMITS.MIN_PLAYERS_ONLINE) {
      return { error: 'Недостаточно подключённых игроков.' };
    }

    this._resetWorldState();
    this.status = ROOM_STATUS.PLAYING;
    this.startedAt = Date.now();
    this.lastActivityAt = Date.now();

    let idx = 0;
    for (const p of this.players.values()) {
      const sp = SPAWN_POINTS[idx % SPAWN_POINTS.length];
      p.x = sp.x;
      p.y = 0;
      p.z = sp.z;
      p.yaw = sp.yaw;
      p.pitch = 0;
      p.speed = 0;
      p.flashlight = true;
      p.crouching = false;
      p.sprinting = false;
      p.composure = 100;
      p.animState = 'idle';
      p.zoneName = getZoneAt(sp.x, sp.z).name;
      p.lastStateAt = Date.now();
      idx++;
    }

    this.broadcast(this._buildGameStartPacket());
    this.startLoop();
    return { ok: true };
  }

  returnToLobby(requesterId) {
    if (requesterId !== this.hostPlayerId) {
      return { error: 'Только хост может вернуть группу в лобби.' };
    }
    this.stopLoop();
    this.status = ROOM_STATUS.LOBBY;
    this.startedAt = null;
    for (const p of this.players.values()) {
      p.ready = false;
      p.composure = 100;
    }
    this.broadcastLobbyState();
    return { ok: true };
  }

  handlePlayerState(playerId, data) {
    if (this.status !== ROOM_STATUS.PLAYING) return;
    const p = this.players.get(playerId);
    if (!p || !p.connected) return;

    const now = Date.now();
    const dt = Math.max(0.016, Math.min(0.5, (now - p.lastStateAt) / 1000));
    p.lastStateAt = now;
    this.lastActivityAt = now;

    const reqX = Number(data.x);
    const reqZ = Number(data.z);
    if (Number.isFinite(reqX) && Number.isFinite(reqZ)) {
      const dx = reqX - p.x;
      const dz = reqZ - p.z;
      const dist = Math.hypot(dx, dz);
      const maxAllowed = LIMITS.MAX_MOVE_SPEED * dt + 0.65;

      let targetX = reqX;
      let targetZ = reqZ;
      if (dist > maxAllowed && dist > 0.001) {
        targetX = p.x + (dx / dist) * maxAllowed;
        targetZ = p.z + (dz / dist) * maxAllowed;
      }

      const closedDoors = this.getClosedDoors();
      const resolved = resolveMapCollision(targetX, targetZ, closedDoors);
      p.speed = Math.hypot(resolved.x - p.x, resolved.z - p.z) / dt;
      p.x = resolved.x;
      p.z = resolved.z;
      p.zoneName = getZoneAt(p.x, p.z).name;
    }

    if (Number.isFinite(Number(data.yaw))) p.yaw = Number(data.yaw);
    if (Number.isFinite(Number(data.pitch))) {
      p.pitch = Math.max(-1.35, Math.min(1.35, Number(data.pitch)));
    }
    if (typeof data.flashlight === 'boolean') p.flashlight = data.flashlight;
    if (typeof data.crouching === 'boolean') p.crouching = data.crouching;
    if (typeof data.sprinting === 'boolean') p.sprinting = data.sprinting;

    if (p.speed > 0.35) {
      p.animState = p.crouching ? 'crouch_walk' : p.sprinting ? 'sprint' : 'walk';
    } else {
      p.animState = p.crouching ? 'crouch_idle' : 'idle';
    }
  }

  handleObjectInteract(playerId, objectId) {
    if (this.status !== ROOM_STATUS.PLAYING) return;
    const player = this.players.get(playerId);
    const obj = this.objects.get(objectId);
    if (!player || !obj) return;

    const dist = Math.hypot(player.x - obj.x, player.z - obj.z);
    if (dist > LIMITS.INTERACT_RANGE) {
      this.sendToPlayer(playerId, { type: MSG.ERROR, message: 'Вы слишком далеко от объекта.' });
      return;
    }

    const closedDoors = this.getClosedDoors();
    if (!hasLineOfSight(player.x, player.z, obj.x, obj.z, closedDoors, obj.type === 'door' ? obj.id : null)) {
      this.sendToPlayer(playerId, { type: MSG.ERROR, message: 'Между вами и объектом стена.' });
      return;
    }

    this.lastActivityAt = Date.now();

    if (obj.type === 'keycard') {
      if (obj.state.pickedUp) return;
      obj.state.pickedUp = true;
      this.hasArchiveKeycard = true;
      const southDoor = this.objects.get('door_south');
      if (southDoor) {
        southDoor.state.locked = false;
        southDoor.label = 'Гермодверь: Южный архив';
        this.broadcastObjectUpdate(southDoor, player.id);
      }
      this.broadcastObjectUpdate(obj, player.id);
      this.broadcastPlayerAction(player.id, 'interact', obj.id);
      this.broadcastSystemNotice(`${player.name} нашёл ключ-карту! Гермодверь Южного архива разблокирована.`);
      return;
    }

    if (obj.type === 'door') {
      if (obj.state.locked) {
        if (obj.state.requiresKeycard) {
          this.sendToPlayer(playerId, {
            type: MSG.SYSTEM_NOTICE,
            text: 'Дверь заблокирована. Найдите ключ-карту в Западной лаборатории или Генераторном блоке.',
          });
          return;
        }
        const needed = obj.state.requiresRelays || MAP_CONFIG.requiredRelays;
        const current = this.getActivatedRelayCount();
        this.sendToPlayer(playerId, {
          type: MSG.SYSTEM_NOTICE,
          text: `Шлюз заблокирован. Активировано реле: ${current}/${needed}.`,
        });
        return;
      }
      obj.state.isOpen = !obj.state.isOpen;
      this.broadcastObjectUpdate(obj, player.id);
      this.broadcastPlayerAction(player.id, 'interact', obj.id);
      return;
    }

    if (obj.type === 'relay') {
      if (obj.state.active) return;
      obj.state.active = true;
      this.broadcastObjectUpdate(obj, player.id);
      this.broadcastPlayerAction(player.id, 'interact', obj.id);

      // Кооперативная синхронизация Турбины А и Турбины Б при игре 2+ человек (20 секунд)
      const isCoop = this.getConnectedPlayers().length >= 2;
      if (isCoop && (obj.id === 'relay_east_1' || obj.id === 'relay_east_2')) {
        const pairId = obj.id === 'relay_east_1' ? 'relay_east_2' : 'relay_east_1';
        const pair = this.objects.get(pairId);
        if (pair && !pair.state.active) {
          clearTimeout(this._turbineSyncTimer);
          this.broadcastSystemNotice(
            `⚡ ${obj.label} запущена! Включите ${pair.label} в течение 20 секунд, иначе защита сбросит ток!`,
          );
          this._turbineSyncTimer = setTimeout(() => {
            if (this.status !== ROOM_STATUS.PLAYING) return;
            if (!pair.state.active && obj.state.active) {
              obj.state.active = false;
              this.broadcastObjectUpdate(obj, null);
              this.broadcast({
                type: MSG.HORROR_EVENT,
                eventId: 'turbine_sync_fail',
                scope: 'global',
                timestamp: Date.now(),
                payload: {
                  effect: 'power_flicker',
                  durationMs: 3200,
                  sound: 'power_drop',
                  subtitle: `Синхронизация турбин сорвалась — ${obj.label} отключилась! Запустите обе почти одновременно.`,
                },
              });
            }
          }, 20_000);
          this._turbineSyncTimer.unref?.();
        } else if (pair && pair.state.active) {
          clearTimeout(this._turbineSyncTimer);
          this._turbineSyncTimer = null;
          this.broadcastSystemNotice('⚡ Обе турбины синхронизированы и зафиксированы!');
        }
      }

      const relayCount = this.getActivatedRelayCount();
      if (relayCount >= MAP_CONFIG.requiredRelays) {
        const airlock = this.objects.get('door_airlock');
        if (airlock && airlock.state.locked) {
          airlock.state.locked = false;
          airlock.state.isOpen = true;
          this.broadcastObjectUpdate(airlock, player.id);
        }
      }

      this.eventEngine.triggerAction('relay_activated', {
        relayCount,
        requiredRelays: MAP_CONFIG.requiredRelays,
        actorName: player.name,
      });
      return;
    }

    if (obj.type === 'note') {
      this.broadcastPlayerAction(player.id, 'read_note', obj.id);
      this.sendToPlayer(playerId, {
        type: MSG.OBJECT_UPDATED,
        object: obj,
        openedNote: obj.state,
      });
      return;
    }

    if (obj.type === 'evac_terminal') {
      if (this.getActivatedRelayCount() < MAP_CONFIG.requiredRelays) {
        this.sendToPlayer(playerId, {
          type: MSG.SYSTEM_NOTICE,
          text: 'Недостаточно питания для запуска подъёмника (нужны все 4 реле).',
        });
        return;
      }
      this.sendToPlayer(playerId, {
        type: MSG.OPEN_KEYPAD,
        prompt: 'Введите 4-значный код эвакуации (цифры из 4 журналов станции):',
      });
    }
  }

  handleSubmitCode(playerId, codeRaw) {
    if (this.status !== ROOM_STATUS.PLAYING) return;
    const player = this.players.get(playerId);
    const term = this.objects.get('evac_terminal');
    if (!player || !term) return;

    const dist = Math.hypot(player.x - term.x, player.z - term.z);
    if (dist > LIMITS.INTERACT_RANGE + 1.0) return;

    if (this.getActivatedRelayCount() < MAP_CONFIG.requiredRelays) {
      this.sendToPlayer(playerId, { type: MSG.ERROR, message: 'Сначала включите все 4 реле питания.' });
      return;
    }

    const cleanCode = String(codeRaw || '').replace(/\D/g, '').slice(0, 4);
    if (cleanCode === this.evacCode) {
      term.state.activated = true;
      this.broadcastObjectUpdate(term, player.id);
      this.endSession(
        'evacuated',
        `Код ${this.evacCode} принят! Группа под руководством ${player.name} запустила аварийный подъёмник и покинула «Объект-9».`,
      );
    } else {
      this.broadcast({
        type: MSG.HORROR_EVENT,
        eventId: 'wrong_evac_code',
        scope: 'global',
        timestamp: Date.now(),
        payload: {
          effect: 'emergency_red',
          durationMs: 4500,
          sound: 'klaxon_distant',
          subtitle: `Неверный код (${cleanCode || '----'})! Сработала тревога шлюза.`,
        },
      });
    }
  }

  handlePlayerInteract(actorId, targetPlayerId, action = 'steady') {
    if (this.status !== ROOM_STATUS.PLAYING) return;
    const actor = this.players.get(actorId);
    const target = this.players.get(targetPlayerId);
    if (!actor || !target || actor.id === target.id) return;

    const dist = Math.hypot(actor.x - target.x, actor.z - target.z);
    if (dist > LIMITS.PLAYER_INTERACT_RANGE) return;
    if (!hasLineOfSight(actor.x, actor.z, target.x, target.z, this.getClosedDoors())) return;

    if (action === 'steady') {
      target.composure = Math.min(100, target.composure + 34);
      actor.composure = Math.min(100, actor.composure + 12);
      this.broadcastPlayerAction(actor.id, 'steady', target.id);
      this.broadcastSystemNotice(`${actor.name} поддержал ${target.name} — паника отступила.`);
    }
  }

  handleRadioPing(playerId) {
    if (this.status !== ROOM_STATUS.PLAYING) return;
    const player = this.players.get(playerId);
    if (!player) return;

    this.broadcast({
      type: MSG.RADIO_SIGNAL,
      playerId: player.id,
      playerName: player.name,
      x: Number(player.x.toFixed(2)),
      z: Number(player.z.toFixed(2)),
      zoneName: player.zoneName,
      timestamp: Date.now(),
    });
  }

  handleChat(playerId, text) {
    const player = this.players.get(playerId);
    if (!player) return;
    const clean = sanitizeText(text, LIMITS.MAX_CHAT_LENGTH);
    if (!clean) return;

    const msg = {
      type: MSG.CHAT_MESSAGE,
      id: crypto.randomBytes(4).toString('hex'),
      senderId: player.id,
      senderName: player.name,
      skinId: player.skinId,
      text: clean,
      timestamp: Date.now(),
    };
    this.chatHistory.push(msg);
    if (this.chatHistory.length > 40) this.chatHistory.shift();
    this.broadcast(msg);
  }

  endSession(outcome, summary) {
    this.stopLoop();
    this.status = ROOM_STATUS.ENDED;
    const durationSec = Math.round(this.getElapsedSeconds());
    this.broadcast({
      type: MSG.SESSION_ENDED,
      outcome,
      summary,
      durationSec,
      relaysActivated: this.getActivatedRelayCount(),
      hostPlayerId: this.hostPlayerId,
    });
  }

  startLoop() {
    this.stopLoop();
    this._tickInterval = setInterval(() => this._tick(), this._tickMs);
  }

  stopLoop() {
    clearTimeout(this._turbineSyncTimer);
    this._turbineSyncTimer = null;
    if (this._tickInterval) {
      clearInterval(this._tickInterval);
      this._tickInterval = null;
    }
  }

  _tick() {
    const now = Date.now();

    for (const [id, p] of this.players.entries()) {
      if (!p.connected && p.disconnectedAt && now - p.disconnectedAt > LIMITS.RECONNECT_GRACE_MS) {
        this.removePlayer(id, 'не смог переподключиться');
      }
    }

    if (this.status !== ROOM_STATUS.PLAYING) return;

    const connected = this.getConnectedPlayers();
    let sprintNoise = 0;
    for (const p of connected) {
      if (p.sprinting && p.speed > 3.5) sprintNoise += 0.08;
      let nearMate = false;
      for (const other of connected) {
        if (other.id !== p.id && Math.hypot(p.x - other.x, p.z - other.z) < 7.5) {
          nearMate = true;
          break;
        }
      }
      if (nearMate && p.composure < 100) {
        p.composure = Math.min(100, p.composure + 0.14);
      }
    }

    this.eventEngine.tick(now, this.getElapsedSeconds());
    this.anomalyDirector.tick(now, connected, Math.min(1, this.eventEngine.tension + sprintNoise));

    this.broadcast({
      type: MSG.WORLD_SNAPSHOT,
      serverTime: now,
      tension: Number(this.eventEngine.tension.toFixed(2)),
      relaysActivated: this.getActivatedRelayCount(),
      hasArchiveKeycard: this.hasArchiveKeycard,
      anomaly: this.anomalyDirector.serialize(),
      players: connected.map((p) => ({
        id: p.id,
        name: p.name,
        skinId: p.skinId,
        x: Number(p.x.toFixed(3)),
        z: Number(p.z.toFixed(3)),
        yaw: Number(p.yaw.toFixed(3)),
        pitch: Number(p.pitch.toFixed(3)),
        flashlight: p.flashlight,
        crouching: p.crouching,
        sprinting: p.sprinting,
        animState: p.animState,
        composure: Math.round(p.composure),
        zoneName: p.zoneName,
      })),
    });
  }

  getConnectedPlayers() {
    return [...this.players.values()].filter((p) => p.connected && p.ws && p.ws.readyState === 1);
  }

  getClosedDoors() {
    const list = [];
    for (const obj of this.objects.values()) {
      if (obj.type === 'door' && !obj.state.isOpen) {
        list.push(obj);
      }
    }
    return list;
  }

  getActivatedRelayCount() {
    let count = 0;
    for (const obj of this.objects.values()) {
      if (obj.type === 'relay' && obj.state.active) count++;
    }
    return count;
  }

  getElapsedSeconds() {
    return this.startedAt ? (Date.now() - this.startedAt) / 1000 : 0;
  }

  _buildGameStartPacket() {
    return {
      type: MSG.GAME_STARTED,
      roomId: this.id,
      hostPlayerId: this.hostPlayerId,
      hasArchiveKeycard: this.hasArchiveKeycard,
      objects: [...this.objects.values()],
      players: this.getConnectedPlayers().map((p) => ({
        id: p.id,
        name: p.name,
        skinId: p.skinId,
        x: p.x,
        z: p.z,
        yaw: p.yaw,
        pitch: p.pitch,
        flashlight: p.flashlight,
        composure: Math.round(p.composure),
        animState: p.animState,
      })),
    };
  }

  broadcastLobbyState() {
    this.broadcast({
      type: MSG.LOBBY_STATE,
      room: this.getPublicSummary(true),
      hostPlayerId: this.hostPlayerId,
      players: [...this.players.values()].map((p) => ({
        id: p.id,
        name: p.name,
        skinId: p.skinId,
        ready: p.ready,
        connected: p.connected,
        isHost: p.id === this.hostPlayerId,
      })),
    });
  }

  broadcastObjectUpdate(obj, actorId = null) {
    this.broadcast({
      type: MSG.OBJECT_UPDATED,
      object: obj,
      actorId,
    });
  }

  broadcastPlayerAction(playerId, action, targetId = null) {
    this.broadcast({
      type: MSG.PLAYER_ACTION,
      playerId,
      action,
      targetId,
      timestamp: Date.now(),
    });
  }

  broadcastSystemNotice(text) {
    this.broadcast({
      type: MSG.SYSTEM_NOTICE,
      text,
      timestamp: Date.now(),
    });
  }

  sendToPlayer(playerId, payload) {
    const p = this.players.get(playerId);
    if (p && p.connected && p.ws && p.ws.readyState === 1) {
      p.ws.sendJSON(payload);
    }
  }

  broadcast(payload) {
    const raw = JSON.stringify(payload);
    for (const p of this.players.values()) {
      if (p.connected && p.ws && p.ws.readyState === 1) {
        p.ws.send(raw);
      }
    }
  }

  getPublicSummary(includeCode = false) {
    return {
      id: this.id,
      name: this.name,
      isPrivate: this.isPrivate,
      inviteCode: includeCode || !this.isPrivate ? this.inviteCode : undefined,
      status: this.status,
      playerCount: this.players.size,
      maxPlayers: this.maxPlayers,
    };
  }
}

function sanitizeText(str, maxLen) {
  if (typeof str !== 'string') return '';
  return str.replace(/[\r\n\t<>]/g, ' ').trim().slice(0, maxLen);
}

// === SERVER: RoomManager.js ===
/**
 * Менеджер игровых комнат: создание публичных и приватных комнат,
 * генерация 6-символьных кодов приглашения, список комнат и очистка пустых.
 */


const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

class RoomManager {
  constructor({ maxRooms = 50, tickRateHz = 15, idleCleanupMs = 120_000 } = {}) {
    this.maxRooms = maxRooms;
    this.tickRateHz = tickRateHz;
    this.rooms = new Map(); // roomId -> GameRoom
    this.codeToRoomId = new Map(); // inviteCode -> roomId

    this._cleanupTimer = setInterval(() => this._cleanupEmptyRooms(idleCleanupMs), 30_000);
    this._cleanupTimer.unref?.();
  }

  createRoom({ name, isPrivate, maxPlayers }) {
    if (this.rooms.size >= this.maxRooms) {
      return { error: 'На сервере достигнут лимит активных комнат. Попробуйте позже.' };
    }

    const id = 'r_' + crypto.randomBytes(4).toString('hex');
    const inviteCode = this._generateUniqueCode();
    const cleanName =
      typeof name === 'string' && name.trim()
        ? name.replace(/[\r\n\t<>]/g, ' ').trim().slice(0, LIMITS.MAX_ROOM_NAME_LENGTH)
        : `Сектор #${inviteCode.slice(0, 3)}`;

    const room = new GameRoom({
      id,
      name: cleanName,
      isPrivate: Boolean(isPrivate),
      inviteCode,
      maxPlayers,
      hostPlayerId: null,
      tickRateHz: this.tickRateHz,
    });

    this.rooms.set(id, room);
    this.codeToRoomId.set(inviteCode, id);
    return { room };
  }

  getRoomById(roomId) {
    return this.rooms.get(roomId) || null;
  }

  getRoomByCode(code) {
    if (typeof code !== 'string') return null;
    const normalized = code.trim().toUpperCase();
    const roomId = this.codeToRoomId.get(normalized);
    return roomId ? this.rooms.get(roomId) || null : null;
  }

  listPublicRooms() {
    const result = [];
    for (const room of this.rooms.values()) {
      if (!room.isPrivate && room.players.size > 0 && room.status !== ROOM_STATUS.ENDED) {
        result.push(room.getPublicSummary(false));
      }
    }
    return result;
  }

  deleteRoomIfEmpty(room) {
    if (!room || room.players.size > 0) return;
    room.stopLoop();
    this.rooms.delete(room.id);
    this.codeToRoomId.delete(room.inviteCode);
  }

  _cleanupEmptyRooms(idleCleanupMs) {
    const now = Date.now();
    for (const room of this.rooms.values()) {
      if (room.players.size === 0 || (room.getConnectedPlayers().length === 0 && now - room.lastActivityAt > idleCleanupMs)) {
        room.stopLoop();
        this.rooms.delete(room.id);
        this.codeToRoomId.delete(room.inviteCode);
      }
    }
  }

  _generateUniqueCode() {
    for (let attempt = 0; attempt < 20; attempt++) {
      let code = '';
      for (let i = 0; i < 6; i++) {
        code += CODE_ALPHABET[crypto.randomInt(0, CODE_ALPHABET.length)];
      }
      if (!this.codeToRoomId.has(code)) return code;
    }
    return crypto.randomBytes(3).toString('hex').toUpperCase();
  }

  shutdown() {
    clearInterval(this._cleanupTimer);
    for (const room of this.rooms.values()) {
      room.stopLoop();
    }
    this.rooms.clear();
    this.codeToRoomId.clear();
  }
}

// === SERVER: index.js ===
/**
 * Точка входа сервера «ЭХО СТАНЦИИ: ОБЪЕКТ-9».
 */



const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = __dirname;
const CLIENT_DIR = __dirname;
const SHARED_DIR = __dirname;

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

function createGameServer(customConfig = {}) {
  const cfg = { ...SERVER_CONFIG, ...customConfig };
  const roomManager = new RoomManager({
    maxRooms: cfg.maxRooms,
    tickRateHz: cfg.tickRateHz,
    idleCleanupMs: cfg.roomIdleCleanupMs,
  });

  const httpServer = http.createServer((req, res) => {
    const reqUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    const pathname = decodeURIComponent(reqUrl.pathname);

    if (pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(
        JSON.stringify({
          status: 'ok',
          protocol: PROTOCOL_VERSION,
          rooms: roomManager.rooms.size,
          uptimeSec: Math.round(process.uptime()),
        }),
      );
      return;
    }

    let targetPath;
    if (pathname.startsWith('/shared/')) {
      const rel = pathname.slice('/shared/'.length);
      targetPath = path.resolve(SHARED_DIR, rel);
      if (!targetPath.startsWith(SHARED_DIR)) {
        res.writeHead(403);
        res.end('Forbidden');
        return;
      }
    } else {
      const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
      targetPath = path.resolve(CLIENT_DIR, rel);
      if (!targetPath.startsWith(CLIENT_DIR)) {
        res.writeHead(403);
        res.end('Forbidden');
        return;
      }
    }

    fs.readFile(targetPath, (err, content) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Not Found');
        return;
      }
      const ext = path.extname(targetPath).toLowerCase();
      res.writeHead(200, {
        'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
        'Cache-Control': 'no-cache',
      });
      res.end(content);
    });
  });

  const wss = new WebSocketServer(httpServer, {
    heartbeatIntervalMs: cfg.heartbeatIntervalMs,
  });

  wss.on('connection', (ws) => {
    const session = {
      room: null,
      playerId: null,
    };

    ws.sendJSON({
      type: MSG.WELCOME,
      protocol: PROTOCOL_VERSION,
      serverTime: Date.now(),
    });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch {
        ws.sendJSON({ type: MSG.ERROR, message: 'Некорректный формат сетевого пакета.' });
        return;
      }

      if (!msg || typeof msg.type !== 'string') return;
      routeMessage(ws, session, msg, roomManager);
    });

    ws.on('close', () => {
      if (session.room && session.playerId) {
        session.room.handleDisconnect(session.playerId, false);
        roomManager.deleteRoomIfEmpty(session.room);
      }
    });
  });

  return {
    httpServer,
    wss,
    roomManager,
    start() {
      return new Promise((resolve) => {
        httpServer.listen(cfg.port, cfg.host, () => {
          const addr = httpServer.address();
          resolve(addr);
        });
      });
    },
    stop() {
      return new Promise((resolve) => {
        wss.close();
        roomManager.shutdown();
        httpServer.close(() => resolve());
      });
    },
  };
}

function routeMessage(ws, session, msg, roomManager) {
  switch (msg.type) {
    case MSG.PING: {
      ws.sendJSON({ type: MSG.PONG, clientTime: msg.clientTime || 0, serverTime: Date.now() });
      return;
    }

    case MSG.LIST_ROOMS: {
      ws.sendJSON({ type: MSG.ROOM_LIST, rooms: roomManager.listPublicRooms() });
      return;
    }

    case MSG.CREATE_ROOM: {
      leaveCurrentRoom(session, roomManager, true);
      const { room, error } = roomManager.createRoom({
        name: msg.roomName,
        isPrivate: msg.isPrivate,
        maxPlayers: msg.maxPlayers,
      });
      if (error) {
        ws.sendJSON({ type: MSG.ERROR, message: error });
        return;
      }
      joinRoomInternal(ws, session, room, { name: msg.playerName, skinId: msg.skinId });
      return;
    }

    case MSG.JOIN_ROOM: {
      const room = roomManager.getRoomById(msg.roomId);
      if (!room) {
        ws.sendJSON({ type: MSG.ERROR, message: 'Комната не найдена или уже закрыта.' });
        return;
      }
      leaveCurrentRoom(session, roomManager, true);
      joinRoomInternal(ws, session, room, { name: msg.playerName, skinId: msg.skinId });
      return;
    }

    case MSG.JOIN_BY_CODE: {
      const room = roomManager.getRoomByCode(msg.inviteCode);
      if (!room) {
        ws.sendJSON({ type: MSG.ERROR, message: 'Комната с таким кодом приглашения не найдена.' });
        return;
      }
      leaveCurrentRoom(session, roomManager, true);
      joinRoomInternal(ws, session, room, { name: msg.playerName, skinId: msg.skinId });
      return;
    }

    case MSG.RECONNECT: {
      const room = roomManager.getRoomById(msg.roomId);
      if (!room) {
        ws.sendJSON({ type: MSG.ERROR, code: 'RECONNECT_FAILED', message: 'Комната больше не существует.' });
        return;
      }
      const { player, error } = room.reconnectPlayer(ws, msg.playerId, msg.reconnectToken);
      if (error) {
        ws.sendJSON({ type: MSG.ERROR, code: 'RECONNECT_FAILED', message: error });
        return;
      }
      session.room = room;
      session.playerId = player.id;
      ws.sendJSON({
        type: MSG.ROOM_JOINED,
        room: room.getPublicSummary(true),
        playerId: player.id,
        reconnectToken: player.reconnectToken,
        reconnected: true,
      });
      return;
    }

    case MSG.LEAVE_ROOM: {
      leaveCurrentRoom(session, roomManager, true);
      ws.sendJSON({ type: MSG.ROOM_LEFT });
      return;
    }

    case MSG.SET_READY: {
      if (session.room && session.playerId) {
        session.room.setPlayerReady(session.playerId, msg.ready);
      }
      return;
    }

    case MSG.UPDATE_PROFILE: {
      if (session.room && session.playerId) {
        session.room.updatePlayerProfile(session.playerId, { name: msg.name, skinId: msg.skinId });
      }
      return;
    }

    case MSG.START_GAME: {
      if (!session.room || !session.playerId) return;
      const res = session.room.startGame(session.playerId);
      if (res.error) ws.sendJSON({ type: MSG.ERROR, message: res.error });
      return;
    }

    case MSG.RETURN_LOBBY: {
      if (!session.room || !session.playerId) return;
      const res = session.room.returnToLobby(session.playerId);
      if (res.error) ws.sendJSON({ type: MSG.ERROR, message: res.error });
      return;
    }

    case MSG.PLAYER_STATE: {
      if (session.room && session.playerId) {
        session.room.handlePlayerState(session.playerId, msg);
      }
      return;
    }

    case MSG.INTERACT_OBJECT: {
      if (session.room && session.playerId) {
        session.room.handleObjectInteract(session.playerId, msg.objectId);
      }
      return;
    }

    case MSG.INTERACT_PLAYER: {
      if (session.room && session.playerId) {
        session.room.handlePlayerInteract(session.playerId, msg.targetPlayerId, msg.action);
      }
      return;
    }

    case MSG.SUBMIT_CODE: {
      if (session.room && session.playerId) {
        session.room.handleSubmitCode(session.playerId, msg.code);
      }
      return;
    }

    case MSG.RADIO_PING: {
      if (session.room && session.playerId) {
        session.room.handleRadioPing(session.playerId);
      }
      return;
    }

    case MSG.CHAT_SEND: {
      if (session.room && session.playerId) {
        session.room.handleChat(session.playerId, msg.text);
      }
      return;
    }

    default:
      break;
  }
}

function joinRoomInternal(ws, session, room, profile) {
  const { player, error } = room.addPlayer(ws, profile);
  if (error) {
    ws.sendJSON({ type: MSG.ERROR, message: error });
    return;
  }
  session.room = room;
  session.playerId = player.id;
  ws.sendJSON({
    type: MSG.ROOM_JOINED,
    room: room.getPublicSummary(true),
    playerId: player.id,
    reconnectToken: player.reconnectToken,
    reconnected: false,
  });
  room.broadcastLobbyState();
}

function leaveCurrentRoom(session, roomManager, graceful) {
  if (!session.room || !session.playerId) return;
  const oldRoom = session.room;
  const oldPlayerId = session.playerId;
  session.room = null;
  session.playerId = null;
  oldRoom.handleDisconnect(oldPlayerId, graceful);
  roomManager.deleteRoomIfEmpty(oldRoom);
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === __filename;
if (isDirectRun) {
  const server = createGameServer();
  server.start().then((addr) => {
    console.log(`[ЭХО СТАНЦИИ: ОБЪЕКТ-9] Сервер запущен: http://${SERVER_CONFIG.host}:${addr.port}`);
  });
}
