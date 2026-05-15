#!/usr/bin/env bash

# Shared env utilities for bash scripts.

load_env_file_without_override() {
  local env_file="$1"
  [[ -f "${env_file}" ]] || return 0

  while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
    raw_line="${raw_line%$'\r'}"
    [[ -z "${raw_line}" ]] && continue
    [[ "${raw_line}" =~ ^[[:space:]]*# ]] && continue
    [[ "${raw_line}" != *=* ]] && continue

    local key="${raw_line%%=*}"
    local value="${raw_line#*=}"

    key="${key//[[:space:]]/}"
    [[ -z "${key}" ]] && continue

    # trim leading/trailing spaces from value
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"

    # strip matching quote pair
    if [[ "${value}" == \"*\" ]] && [[ "${value}" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'*\' ]]; then
      value="${value:1:${#value}-2}"
    fi

    # Important: do not override variables already set by CLI/env.
    if [[ -z "${!key+x}" ]]; then
      export "${key}=${value}"
    fi
  done < "${env_file}"
}

ensure_postgres_password() {
  local env_file="$1"
  local postgres_container="${2:-welding-postgres}"
  local fallback_value="${3:-welding_local_auto_pw}"

  if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
    export POSTGRES_PASSWORD
    return 0
  fi

  # 1) try .env file value (without overriding other vars)
  load_env_file_without_override "${env_file}"
  if [[ -n "${POSTGRES_PASSWORD:-}" ]]; then
    export POSTGRES_PASSWORD
    return 0
  fi

  # 2) try container env (when already running)
  if command -v docker >/dev/null 2>&1; then
    local discovered=""
    discovered="$(
      docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "${postgres_container}" 2>/dev/null \
        | awk -F= '$1=="POSTGRES_PASSWORD"{print $2; exit}'
    )"
    if [[ -n "${discovered}" ]]; then
      export POSTGRES_PASSWORD="${discovered}"
      return 0
    fi
  fi

  # 3) auto-generate deterministic local fallback for demo/experiment usability
  export POSTGRES_PASSWORD="${fallback_value}"

  # persist into .env for docker compose interpolation if possible
  if [[ -n "${env_file}" ]]; then
    if [[ ! -f "${env_file}" ]]; then
      printf 'POSTGRES_PASSWORD=%s\n' "${POSTGRES_PASSWORD}" > "${env_file}"
      return 0
    fi
    if grep -q '^POSTGRES_PASSWORD=' "${env_file}" 2>/dev/null; then
      sed -i.bak "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${POSTGRES_PASSWORD}|" "${env_file}" || true
      rm -f "${env_file}.bak" 2>/dev/null || true
    else
      printf '\nPOSTGRES_PASSWORD=%s\n' "${POSTGRES_PASSWORD}" >> "${env_file}"
    fi
  fi
  return 0
}

