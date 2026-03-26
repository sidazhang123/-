/**
 * param-render.js — 只读参数渲染工具（从 monitor.js 提取）。
 *
 * 暴露全局对象 ParamRender，包含：
 *   escapeHtml, helpTextFromNode, pathToString, getValueAtPath,
 *   renderParamField, renderInlineTemplateSection
 *
 * 被 monitor.js / results.js / task-manager.js 共同引用。
 */
/* eslint-disable no-unused-vars */
const ParamRender = (() => {
  "use strict";

  function escapeHtml(text) {
    return String(text || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function helpTextFromNode(node) {
    if (!node) return "";
    if (typeof node === "string") return node;
    if (Array.isArray(node)) return node.map((item) => String(item)).join("\n");
    if (typeof node === "object") {
      const blocks = [];
      if (typeof node._comment === "string" && node._comment.trim()) {
        blocks.push(node._comment.trim());
      }
      if (Array.isArray(node._overview) && node._overview.length) {
        blocks.push(node._overview.map((item) => String(item)).join("\n"));
      }
      return blocks.join("\n\n");
    }
    return "";
  }

  function pathToString(path) {
    return path.join(".");
  }

  function getValueAtPath(root, path) {
    let current = root;
    for (const key of path) {
      if (!current || typeof current !== "object") return undefined;
      current = current[key];
    }
    return current;
  }

  // 前端显示名映射
  const _LABEL_MAP = {
    "concepts": "概念预筛选",
    "concept_terms": "概念名称",
    "reason_terms": "入选理由",
    "filter_st": "ST股票过滤",
  };
  const _UNWRAP_SECTIONS = new Set(["universe_filters"]);
  const _HIDE_LABEL_KEYS = new Set(["universe_filters.concepts.enabled"]);

  function renderParamField(path, value, helpNode, labelText, depth = 0, opts = {}) {
    const readonly = opts.readonly === true;
    const pathKey = pathToString(path);
    if (labelText in _LABEL_MAP) labelText = _LABEL_MAP[labelText];
    const hideLabelAndHelp = _HIDE_LABEL_KEYS.has(pathKey);
    const helpText = hideLabelAndHelp ? "" : helpTextFromNode(helpNode);

    if (Array.isArray(value)) {
      const isStringArray = value.every((item) => typeof item === "string");
      if (!isStringArray) {
        return `
          <div class="param-field unsupported">
            <div class="param-label-row">
              <div class="param-label">${escapeHtml(labelText)}</div>
            </div>
            ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
            <div class="param-readonly">暂不支持的数组类型</div>
          </div>
        `;
      }
      if (readonly) {
        const tags = value.map((v) => `<span class="param-tag-readonly">${escapeHtml(v)}</span>`).join("");
        return `
          <div class="param-field">
            <div class="param-label-row">
              <div class="param-label">${escapeHtml(labelText)}</div>
            </div>
            ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
            <div class="param-tags-readonly">${tags || '<span class="param-tag-empty">（空）</span>'}</div>
          </div>
        `;
      }
      return `
        <div class="param-field">
          <div class="param-label-row">
            <div class="param-label">${escapeHtml(labelText)}</div>
          </div>
          ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
          <div class="param-tags-editor" data-param-path="${escapeHtml(pathKey)}"></div>
        </div>
      `;
    }

    if (value && typeof value === "object") {
      if (helpNode && helpNode._render === "inline_template") {
        return renderInlineTemplateSection(path, value, helpNode, opts);
      }
      const lastKey = path.length ? path[path.length - 1] : "";
      if (_UNWRAP_SECTIONS.has(lastKey)) {
        const childKeys = Object.keys(value);
        return childKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth, opts)).join("");
      }
      const childKeys = Object.keys(value);
      if (readonly && childKeys.includes("enabled") && value.enabled === false) return "";
      const hasEnabled = childKeys.includes("enabled");
      const singleBoolKey = (!hasEnabled && depth > 0 && childKeys.length === 1 && typeof value[childKeys[0]] === "boolean") ? childKeys[0] : null;
      if (singleBoolKey) {
        const boolPath = path.concat(singleBoolKey);
        const boolKey = pathToString(boolPath);
        const boolVal = value[singleBoolKey] === true;
        const childHelp = helpTextFromNode(helpNode?.[singleBoolKey]);
        if (readonly) {
          if (!boolVal) return "";
          return `
            <section class="param-section param-drawer" data-param-section="${escapeHtml(pathKey)}">
              <div class="param-drawer-header">
                <div class="param-section-title">${escapeHtml(labelText)}</div>
              </div>
              ${(helpText || childHelp) ? `<div class="param-help">${escapeHtml(helpText || childHelp)}</div>` : ""}
            </section>
          `;
        }
        return `
          <section class="param-section param-drawer" data-param-section="${escapeHtml(pathKey)}">
            <div class="param-drawer-header">
              <div class="param-section-title">${escapeHtml(labelText)}</div>
              <span class="param-checkbox-row">
                <input type="checkbox" data-param-path="${escapeHtml(boolKey)}" ${boolVal ? "checked" : ""} />
                <span class="param-checkbox-value">${boolVal ? "开启" : "关闭"}</span>
              </span>
            </div>
            ${(helpText || childHelp) ? `<div class="param-help">${escapeHtml(helpText || childHelp)}</div>` : ""}
          </section>
        `;
      }
      if (hasEnabled) {
        const isEnabled = value.enabled === true;
        const enabledPath = path.concat("enabled");
        const enabledKey = pathToString(enabledPath);
        const bodyKeys = childKeys.filter((k) => k !== "enabled");
        if (readonly) {
          if (!isEnabled) return "";
          return `
            <section class="param-section param-drawer" data-param-section="${escapeHtml(pathKey)}">
              <div class="param-drawer-header">
                <div class="param-section-title">${escapeHtml(labelText)}</div>
              </div>
              ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
              <div class="param-drawer-body" data-drawer-section="${escapeHtml(pathKey)}">
                <div class="param-section-body">
                  ${bodyKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth + 1, opts)).join("")}
                </div>
              </div>
            </section>
          `;
        }
        return `
          <section class="param-section param-drawer" data-param-section="${escapeHtml(pathKey)}">
            <div class="param-drawer-header">
              <div class="param-section-title">${escapeHtml(labelText)}</div>
              <span class="param-checkbox-row">
                <input type="checkbox" data-param-path="${escapeHtml(enabledKey)}" ${isEnabled ? "checked" : ""} />
                <span class="param-checkbox-value">${isEnabled ? "开启" : "关闭"}</span>
              </span>
            </div>
            ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
            <div class="param-drawer-body ${isEnabled ? "" : "param-drawer-closed"}" data-drawer-section="${escapeHtml(pathKey)}">
              <div class="param-section-body">
                ${bodyKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth + 1, opts)).join("")}
              </div>
            </div>
          </section>
        `;
      }
      const titleClass = depth === 0 ? "param-section-title root" : "param-section-title";
      const childrenHtml = childKeys.map((key) => renderParamField(path.concat(key), value[key], helpNode?.[key], key, depth + 1, opts)).join("");
      if (readonly && !childrenHtml.trim()) return "";
      return `
        <section class="param-section ${depth === 0 ? "is-root" : ""}" data-param-section="${escapeHtml(pathKey)}">
          ${labelText ? `<div class="${titleClass}">${escapeHtml(labelText)}</div>` : ""}
          ${helpText ? `<div class="param-help">${escapeHtml(helpText)}</div>` : ""}
          <div class="param-section-body">
            ${childrenHtml}
          </div>
        </section>
      `;
    }

    if (typeof value === "boolean") {
      if (readonly) {
        if (!value) return "";
        return `
          <div class="param-field">
            ${hideLabelAndHelp ? "" : `<span class="param-label">${escapeHtml(labelText)}</span>`}
            ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
          </div>
        `;
      }
      return `
        <div class="param-field checkbox-field">
          ${hideLabelAndHelp ? "" : `<span class="param-label">${escapeHtml(labelText)}</span>`}
          ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
          <span class="param-checkbox-row">
            <input type="checkbox" data-param-path="${escapeHtml(pathKey)}" ${value ? "checked" : ""} />
            <span class="param-checkbox-value">${value ? "开启" : "关闭"}</span>
          </span>
        </div>
      `;
    }

    if (typeof value === "number") {
      const step = Number.isInteger(value) ? "1" : "any";
      if (readonly) {
        return `
          <label class="param-field">
            <span class="param-label">${escapeHtml(labelText)}</span>
            ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
            <span class="param-readonly-value">${escapeHtml(String(value))}</span>
          </label>
        `;
      }
      return `
        <label class="param-field">
          <span class="param-label">${escapeHtml(labelText)}</span>
          ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
          <input
            type="number"
            data-param-path="${escapeHtml(pathKey)}"
            data-param-number-kind="${Number.isInteger(value) ? "int" : "float"}"
            step="${step}"
            value="${escapeHtml(String(value))}"
          />
        </label>
      `;
    }

    if (readonly) {
      return `
        <label class="param-field">
          <span class="param-label">${escapeHtml(labelText)}</span>
          ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
          <span class="param-readonly-value">${escapeHtml(String(value ?? ""))}</span>
        </label>
      `;
    }
    return `
      <label class="param-field">
        <span class="param-label">${escapeHtml(labelText)}</span>
        ${helpText ? `<span class="param-help">${escapeHtml(helpText)}</span>` : ""}
        <input type="text" data-param-path="${escapeHtml(pathKey)}" value="${escapeHtml(String(value ?? ""))}" />
      </label>
    `;
  }

  function renderInlineTemplateSection(path, sectionValue, helpNode, opts = {}) {
    const readonly = opts.readonly === true;
    const sectionKey = pathToString(path);
    const label = helpNode._label || sectionKey;
    const templates = Array.isArray(helpNode._templates) ? helpNode._templates : [];
    const hasEnabled = "enabled" in sectionValue;
    const isEnabled = hasEnabled && sectionValue.enabled === true;

    let headerHtml;
    if (hasEnabled) {
      if (readonly && !isEnabled) return "";
      const enabledPath = path.concat("enabled");
      const enabledKey = pathToString(enabledPath);
      if (readonly) {
        headerHtml = `
          <div class="param-inline-section-header">
            <span class="param-inline-section-title">${escapeHtml(label)}</span>
          </div>
        `;
      } else {
        headerHtml = `
          <div class="param-inline-section-header">
            <span class="param-inline-section-title">${escapeHtml(label)}</span>
            <span class="param-checkbox-row" style="cursor:default">
              <input type="checkbox" data-param-path="${escapeHtml(enabledKey)}" ${isEnabled ? "checked" : ""} style="cursor:pointer" />
              <span class="param-checkbox-value">${isEnabled ? "开启" : "关闭"}</span>
            </span>
          </div>
        `;
      }
    } else {
      headerHtml = `
        <div class="param-inline-section-header">
          <span class="param-inline-section-title">${escapeHtml(label)}</span>
        </div>
      `;
    }

    let rowsHtml = "";
    for (const tpl of templates) {
      const text = tpl.text || "";
      const isRequired = tpl.required === true;
      const toggleField = tpl.toggle || null;
      const followToggleField = tpl.follow_toggle || null;
      const helpTip = tpl.help || "";

      const segments = text.split(/\{(\w+)\}/g);
      let lineHtml = "";
      for (let i = 0; i < segments.length; i += 1) {
        if (i % 2 === 0) {
          if (segments[i]) {
            lineHtml += `<span class="param-inline-text">${escapeHtml(segments[i])}</span>`;
          }
        } else {
          const fieldName = segments[i];
          const fieldPath = path.concat(fieldName);
          const fieldKey = pathToString(fieldPath);
          const fieldVal = sectionValue[fieldName];
          if (readonly) {
            lineHtml += `<span class="param-readonly-value">${escapeHtml(String(fieldVal ?? ""))}</span>`;
          } else if (typeof fieldVal === "number") {
            const step = Number.isInteger(fieldVal) ? "1" : "any";
            const kind = Number.isInteger(fieldVal) ? "int" : "float";
            lineHtml += `<input type="number" class="param-inline-field"
              data-param-path="${escapeHtml(fieldKey)}"
              data-param-number-kind="${kind}"
              step="${step}"
              value="${escapeHtml(String(fieldVal))}" />`;
          } else {
            lineHtml += `<input type="text" class="param-inline-field"
              data-param-path="${escapeHtml(fieldKey)}"
              value="${escapeHtml(String(fieldVal ?? ""))}" />`;
          }
        }
      }

      const badge = isRequired
        ? '<span class="param-inline-badge required">必选</span>'
        : '<span class="param-inline-badge optional">可选</span>';

      if (toggleField) {
        const togglePath = path.concat(toggleField);
        const toggleKey = pathToString(togglePath);
        const toggleVal = sectionValue[toggleField] === true;
        const disabledClass = toggleVal ? "" : "param-inline-toggle-disabled";
        if (readonly) {
          if (!toggleVal) { continue; }
          rowsHtml += `
            <div class="param-inline-row">
              ${badge} ${lineHtml}
              ${helpTip ? `<span class="param-inline-help">${escapeHtml(helpTip)}</span>` : ""}
            </div>
          `;
        } else {
          rowsHtml += `
            <div class="param-inline-toggle-row" data-inline-toggle="${escapeHtml(toggleKey)}">
              <input type="checkbox" data-param-path="${escapeHtml(toggleKey)}" ${toggleVal ? "checked" : ""} />
              <div class="param-inline-toggle-content ${disabledClass}" data-inline-toggle-body="${escapeHtml(toggleKey)}">
                ${badge} ${lineHtml}
                ${helpTip ? `<span class="param-inline-help">${escapeHtml(helpTip)}</span>` : ""}
              </div>
            </div>
          `;
        }
      } else if (followToggleField) {
        const ftPath = path.concat(followToggleField);
        const ftKey = pathToString(ftPath);
        const ftVal = sectionValue[followToggleField] === true;
        if (readonly && !ftVal) { continue; }
        const disabledClass = ftVal ? "" : "param-inline-toggle-disabled";
        rowsHtml += `
          <div class="param-inline-row ${disabledClass}" data-inline-toggle-body="${escapeHtml(ftKey)}">
            ${badge} ${lineHtml}
            ${helpTip ? `<span class="param-inline-help">${escapeHtml(helpTip)}</span>` : ""}
          </div>
        `;
      } else {
        rowsHtml += `<div class="param-inline-row">${badge} ${lineHtml}</div>`;
        if (helpTip) {
          rowsHtml += `<div class="param-inline-help">${escapeHtml(helpTip)}</div>`;
        }
      }
    }

    if (hasEnabled) {
      const bodyDrawerClass = isEnabled ? "" : "param-drawer-closed";
      return `
        <section class="param-inline-section param-drawer" data-param-section="${escapeHtml(sectionKey)}">
          ${headerHtml}
          <div class="param-drawer-body ${bodyDrawerClass}" data-drawer-section="${escapeHtml(sectionKey)}">
            <div class="param-inline-section-body">
              ${rowsHtml}
            </div>
          </div>
        </section>
      `;
    }
    return `
      <section class="param-inline-section" data-param-section="${escapeHtml(sectionKey)}">
        ${headerHtml}
        <div class="param-inline-section-body">
          ${rowsHtml}
        </div>
      </section>
    `;
  }

  return {
    escapeHtml,
    helpTextFromNode,
    pathToString,
    getValueAtPath,
    renderParamField,
    renderInlineTemplateSection,
  };
})();
