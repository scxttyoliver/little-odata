// Version 2.2.5
// Rollback of cooldown to prevent multiple data sources from colliding
// Improved comaptibiltiy with multiple calls to allow concurrent helpers
// Further prevention of premature state advancement

// =====================================
// Globals
// =====================================

var RE_DATE = /^\d{4}-\d{2}-\d{2}$/;
var RE_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/;
var RE_NUMBER = /^-?\d+(?:\.\d+)?$/;
var BACKOFF_MS = 2000;
var MODIFIED_PASS = 1;

var odata_tokens = {};
var odata_abort = new AbortController();

// =====================================
// Error helper
// =====================================

var ERR_CONFIG = "CONFIG_ERROR";
var ERR_AUTH = "AUTH_ERROR";
var ERR_NET = "NETWORK_ERROR";
var ERR_ODATA = "ODATA_ERROR";
var ERR_ABORT = "ABORTED";

function logError(stage, code, info)
{
	console.error("ODATA:", stage, code, info);
}

function parseError(data)
{
	// Error handling for api
	if (!data || typeof data !== "object" || !data.error) return null;

	var err = data.error;
	var code = (err.code || "").toString();
	var msg = (err.message && (err.message.value || err.message)) || (err.details || err.error) || "Unknown ODATA error";

	if (code === "401" || code === "403" || /unauthoriz|auth/i.test(msg)) return { code: ERR_AUTH, message: msg };
	if (/could not find a property/i.test(msg) || /property named/i.test(msg)) return { code: ERR_ODATA, message: "Field not found " + msg };
	if (/resource could not be found.*segment/i.test(msg) || /not found/i.test(msg)) return { code: ERR_ODATA, message: "Table or path not found " + msg };

	return { code: ERR_ODATA, message: msg };
}

// =====================================
// Local state
// =====================================

function syncKey(file, table)
{
	return file + "-" + table + "-sync";
}

function loadSync(file, table)
{
	var key = syncKey(file, table);
	try
	{
		var raw = localStorage.getItem(key);
		if (!raw) return { serial_last: null, sync_timestamp: null, sync_complete: false };

		var state = JSON.parse(raw);
		if (!state || typeof state !== "object") throw new Error("Invalid sync state");

		if (!("serial_last" in state)) state.serial_last = null;
		if (!("sync_timestamp" in state)) state.sync_timestamp = null;
		if (!("sync_complete" in state)) state.sync_complete = false;

		return state;
	}
	catch (_e)
	{
		try { localStorage.removeItem(key); } catch (_err) {}
		return { serial_last: null, sync_timestamp: null, sync_complete: false };
	}
}

function saveSync(file, table, state)
{
	try { localStorage.setItem(syncKey(file, table), JSON.stringify(state)); } catch (_e) {}
}

// =====================================
// Shared helpers
// =====================================

function getSerial(rows, field, current)
{
	var hi = current;
	for (var i = 0; i < rows.length; i++)
	{
		var v = Number(rows[i][field]);
		if (isFinite(v) && (hi == null || v > hi)) hi = v;
	}
	return hi;
}

function checkHalt(instance, token)
{
	const active = (odata_tokens[instance] === token);
	const aborted = (typeof odata_abort?.signal?.aborted === "boolean") ? odata_abort.signal.aborted : false;
	return (active && (aborted || callProxy.error === true || callProxy.fetch_aborted === true));
}

function encodeField(field)
{
	return '"' + String(field).replace(/"/g, '""') + '"';
}

function encodeValue(val)
{
	if (val === null || val === undefined) return "null";
	if (val instanceof Date) return val.toISOString();
	if (typeof val === "string" && (RE_DATE.test(val) || RE_TIMESTAMP.test(val))) return val;
	if (typeof val === "string") return "'" + val.replace(/'/g, "''") + "'";
	if (typeof val !== "number" && typeof val !== "boolean") return "'" + String(val) + "'";
	return val;
}

function getHeaders(url, config)
{
	var proxy_url = config.proxy || "https://proxy.littleman.com.au";
	return { url: proxy_url + "/odata/get?url=" + encodeURIComponent(url), headers: { "fm-username": config.username, "fm-password": config.password } };
}

function getURL(server, file, table, query_parts, limit)
{
	var base = "https://" + server + "/fmi/odata/v4/" + file + "/" + table;
	var params = [];

	if (query_parts.field_select) params.push("$select=" + query_parts.field_select);
	if (query_parts.field_expand) params.push(query_parts.field_expand);
	if (query_parts.field_filter) params.push(query_parts.field_filter);
	if (limit) params.push("$top=" + limit);

	return params.length ? base + "?" + params.join("&") : base;
}

// =====================================
// Query builder
// =====================================

function queryBuilder(select_cfg, filter_cfg, base_table)
{
	function processFilter(field, condition)
	{
		var fld = encodeField(field);

		if (Array.isArray(condition))
		{
			var ors = new Array(condition.length);
			for (var i = 0; i < condition.length; i++) ors[i] = fld + " eq " + encodeValue(condition[i]);
			return "(" + ors.join(" or ") + ")";
		}

		if (condition && typeof condition === "object")
		{
			var parts = [];
			for (var op in condition)
			{
				if (!Object.prototype.hasOwnProperty.call(condition, op)) continue;
				var val = condition[op];
				parts.push(op === "contains" ? "contains(" + fld + "," + encodeValue(val) + ")" : fld + " " + op + " " + encodeValue(val));
			}
			return parts.join(" and ");
		}

		return fld + " eq " + encodeValue(condition);
	}

	var filter_parts = [];
	var filter_keys = filter_cfg ? Object.keys(filter_cfg) : [];
	for (var i = 0; i < filter_keys.length; i++)
	{
		var key = filter_keys[i];
		filter_parts.push(processFilter(key, filter_cfg[key]));
	}
	var filter_string = filter_parts.length ? "$filter=" + encodeURIComponent(filter_parts.join(" and ")) : "";

	var select_list = [];
	var expand_list = [];

	var tables = Object.keys(select_cfg || {});
	for (var t = 0; t < tables.length; t++)
	{
		var table = tables[t];
		var fields = Object.keys(select_cfg[table] || {});
		var encoded = new Array(fields.length);
		for (var j = 0; j < fields.length; j++) encoded[j] = encodeField(fields[j]);

		if (table === base_table) select_list.push(encoded.join(","));
		else expand_list.push('"' + table + '"($select=' + encoded.join(",") + ")");
	}

	return { field_select: encodeURIComponent(select_list.join(",")), field_expand: expand_list.length ? "$expand=" + expand_list.join(",") : "", field_filter: filter_string };
}

// =====================================
// Mapping
// =====================================

function compileMapping(select_cfg, base_table)
{
	var mapping = [];
	var tables = Object.keys(select_cfg || {});
	for (var i = 0; i < tables.length; i++)
	{
		var table = tables[i];
		var fields = select_cfg[table];
		var path_source = (table === base_table) ? null : table;

		for (var fm in fields)
		{
			if (!Object.prototype.hasOwnProperty.call(fields, fm)) continue;
			var rule = fields[fm];
			var key = (rule && typeof rule === "object") ? rule.key : rule;
			var is_arr = !!(rule && typeof rule === "object" && rule.isArray);
			var delim = (rule && typeof rule === "object" && rule.delimiter) ? rule.delimiter : "\n";
			mapping.push({ fm: fm, key: key, path: path_source, arr: is_arr, delim: delim });
		}
	}
	return mapping;
}

function normaliseTypes(key, value, types)
{
	if (!types) return value;

	var want = types[key];
	if (!want) return value;

	if (want === "number")
	{
		if (typeof value === "number") return value;
		if (typeof value === "string" && value.length <= 15 && RE_NUMBER.test(value)) return Number(value);
		return value;
	}

	if (want === "timestamp")
	{
		if (value instanceof Date) return value;
		if (typeof value === "string" && RE_TIMESTAMP.test(value)) return new Date(value);
		if (typeof value === "string" && RE_DATE.test(value)) return new Date(value + "T00:00:00");
		return value;
	}

	if (want === "date")
	{
		if (value instanceof Date) return new Date(value.getFullYear(), value.getMonth(), value.getDate());
		if (typeof value === "string" && RE_DATE.test(value)) return new Date(value + "T00:00:00");
		if (typeof value === "string" && RE_TIMESTAMP.test(value))
		{
			var out = new Date(value);
			return new Date(out.getFullYear(), out.getMonth(), out.getDate());
		}
		return value;
	}

	return value;
}

function applyMapping(rows, mapping, types)
{
	var out = new Array(rows.length);

	for (var i = 0; i < rows.length; i++)
	{
		var rec = rows[i];
		var obj = {};

		for (var m = 0; m < mapping.length; m++)
		{
			var rule = mapping[m];
			var src = rule.path ? (Array.isArray(rec[rule.path]) ? (rec[rule.path][0] || {}) : (rec[rule.path] || {})) : rec;
			var v = src[rule.fm];

			if (rule.arr && typeof v === "string")
			{
				if (rule.delim === "\n") v = v.replace(/\r\n|\r/g, "\n");
				var parts = v.split(rule.delim);
				var a = [];
				for (var k = 0; k < parts.length; k++)
				{
					var s = parts[k].trim();
					if (s) a.push(s);
				}
				obj[rule.key] = a;
			}
			else
			{
				obj[rule.key] = (rule.fm in src) ? normaliseTypes(rule.key, v, types) : null;
			}
		}

		out[i] = obj;
	}

	return out;
}

// =====================================
// Proxy fetch
// =====================================

async function callProxy(url, config, token)
{
	var proxy = getHeaders(url, config);

	try
	{
		if (!config || !config.username || !config.password) throw { code: ERR_CONFIG, message: "Missing credentials" };
		if (!config.server || !config.file || !config.table) throw { code: ERR_CONFIG, message: "Missing server/file/table" };

		var instance = config.instance || "default";
		if (token && odata_tokens[instance] !== token)
		{
			console.warn("Session token mismatch aborting batch");
			return [];
		}

		var res = await fetch(proxy.url, { headers: proxy.headers, signal: odata_abort.signal });
		if (!res.ok)
		{
			// Network errors
			if (res.status === 401 || res.status === 403) throw { code: ERR_AUTH, message: "HTTP " + res.status + " Unauthorized" };
			if (res.status === 404) throw { code: ERR_ODATA, message: "Check file/table/path)" };
			throw { code: ERR_NET, message: "HTTP " + res.status };
		}

		var data = await res.json();

		// ODATA errors
		var oerr = parseError(data);
		if (oerr) throw oerr;

		return data.value || data || [];
	}
	catch (e)
	{
		if (e && (e.name === "AbortError" || e.message === "Load failed"))
		{
			console.warn("Fetch aborted");
			callProxy.fetch_aborted = true;
			return [];
		}

		var code = (e && e.code) ? e.code : ERR_NET;
		var msg = (e && e.message) ? e.message : String(e);

		logError("fetch", code, msg);
		callProxy.error = true;
		callProxy.last_error = msg;

		return [];
	}
}

// =====================================
// Count query
// =====================================

async function getCount(config, mode, token)
{
	var table = config.table, file = config.file, server = config.server;
	var count_field = (config.mode && config.mode.count_field) ? config.mode.count_field : "_Count";
	var select_fields = {}; select_fields[table] = {}; select_fields[table][count_field] = count_field;

	var count_filter = {};
	var base_filter = (config.filter || {});
	for (var k in base_filter) if (Object.prototype.hasOwnProperty.call(base_filter, k)) count_filter[k] = base_filter[k];

	if (mode === "modified")
	{
		var state = loadSync(file, table);
		if (state && state.sync_timestamp)
		{
			var modify_field = (config.mode && config.mode.modify_field) ? config.mode.modify_field : "Timestamp Modify";
			count_filter[modify_field] = { ge: state.sync_timestamp };
		}
	}

	var qp = queryBuilder(select_fields, count_filter, table);
	var url = getURL(server, file, table, qp, 1);

	var rows = await callProxy(url, config, token);

	return (Array.isArray(rows) && rows.length > 0) ? rows[0][count_field] : 0;
}

// =====================================
// Data fetch
// =====================================

function dispatchBatch(hooks, has_batch_hook, mapped, results, serial_last, instance, token)
{
	if (has_batch_hook)
	{
		if (odata_tokens[instance] !== token) return undefined;
		var payload = (serial_last == null) ? { data: mapped, size: mapped.length } : { data: mapped, size: mapped.length, serial_last: serial_last };
		try { return hooks.onBatch(payload); } catch (_e) { return undefined; }
	}
	results.push.apply(results, mapped);
	return undefined;
}

async function getData(config, token)
{
	var table = config.table, file = config.file, server = config.server, mode = config.mode || {};
	var instance = config.instance || "default";
	var limit = (typeof mode.limit === "number" && mode.limit > 0) ? (mode.limit | 0) : 10000;
	var serial_field = (typeof mode.serial_field === "string" && mode.serial_field.trim()) ? mode.serial_field.trim() : null;
	var modify_field = (typeof mode.modify_field === "string" && mode.modify_field.trim()) ? mode.modify_field.trim() : null;
	var types = (config.types && typeof config.types === "object") ? config.types : null;
	var hooks = (config.hooks && typeof config.hooks === "object") ? config.hooks : null;
	var has_batch = !!(hooks && typeof hooks.onBatch === "function");

	// Config validation
	if (!config.select || !config.select[table] || typeof config.select[table] !== "object")
	{
		if (hooks && typeof hooks.onError === "function") { try { hooks.onError({ stage: "config", error: "Missing base table: " + table }); } catch (_e) {} }
		return undefined;
	}
	if (modify_field && !serial_field)
	{
		if (hooks && typeof hooks.onError === "function") { try { hooks.onError({ stage: "config", error: "Serial field is required for sync" }); } catch (_e) {} }
		return undefined;
	}

	// Ensure serial field selected when needed
	if (serial_field && !config.select[table][serial_field]) config.select[table][serial_field] = serial_field;

	var map_rules = compileMapping(config.select, table);
	var base_filter = (config.filter || {});
	var state = modify_field ? loadSync(file, table) : {};
	var serial_last = (modify_field && state.serial_last != null) ? Number(state.serial_last) : null;
	var sync_type = modify_field ? (!state.sync_complete ? "full" : "modified") : "live";
	var ts = new Date(Date.now() - BACKOFF_MS);

	if (hooks && typeof hooks.onStart === "function" && odata_tokens[instance] === token) try { hooks.onStart({ sync: sync_type }); } catch (_e) {}
	if (modify_field && sync_type === "full" && !state.sync_timestamp && odata_tokens[instance] === token)
	{
		state.sync_timestamp = ts;
		saveSync(file, table, state);
	}

	var results = has_batch ? undefined : [];
	var pending = [];

	function isPromise(v){ return v && typeof v.then === "function"; }

	// Full or live
	if (sync_type === "full" || sync_type === "live")
	{
		if (!serial_field)
		{
			if (!checkHalt(instance, token))
			{
				var qp = queryBuilder(config.select, base_filter, table);
				var url = getURL(server, file, table, qp, limit);
				var rows = await callProxy(url, config, token);
				if (rows && rows.length)
				{
					var mapped = applyMapping(rows, map_rules, types);
					var r = dispatchBatch(hooks, has_batch, mapped, results, null, instance, token);
					if (isPromise(r)) pending.push(r);
				}
			}
		}
		else
		{
			var got = 0, rows;
			do
			{
				if (checkHalt(instance, token)) break;

				var bf = {};
				for (var k in base_filter) if (Object.prototype.hasOwnProperty.call(base_filter, k)) bf[k] = base_filter[k];
				if (serial_last !== null) bf[serial_field] = { gt: serial_last };

				var qp = queryBuilder(config.select, bf, table);
				var url = getURL(server, file, table, qp, limit);
				rows = await callProxy(url, config, token);

				if (!Array.isArray(rows) || rows.length === 0)
				{
					if (checkHalt(instance, token)) break;
					break;
				}

				var mapped = applyMapping(rows, map_rules, types);
				var batch_max = getSerial(rows, serial_field, serial_last);

				var r = dispatchBatch(hooks, has_batch, mapped, results, batch_max, instance, token);
				if (isPromise(r)) { try { await r; } catch (_e) {} }
				if (checkHalt(instance, token)) break;

				serial_last = batch_max;
				got += rows.length;

				if (modify_field && odata_tokens[instance] === token)
				{
					state.serial_last = serial_last;
					saveSync(file, table, state);
				}
			}
			while (rows.length === limit);

			// Mark full sync complete only when last page fetched cleanly
			if (modify_field && odata_tokens[instance] === token)
			{
				const is_valid = (callProxy.fetch_aborted === false && callProxy.error !== true);
				const is_final_batch = (sync_type === "full" && Array.isArray(rows) && rows.length < limit);

				if (is_valid && is_final_batch)
				{
					state.sync_complete = true;
				}

				// Persist if the run was valid OR we have any meaningful progress/anchor persisted.
				if (is_valid || state.serial_last != null || state.sync_timestamp != null)
				{
					saveSync(file, table, state);
				}
			}
		}
	}

	// Modified
	if (modify_field && sync_type === "modified")
	{
		var keep = true, loops = 0;
		while (keep)
		{
			if (checkHalt(instance, token)) break;

			var cycle_start = new Date(Date.now() - BACKOFF_MS);
			var processed = 0;
			var page_serial = null;
			var ok = true;
			var rowsm;

			do
			{
				if (checkHalt(instance, token)) { ok = false; break; }

				var bf = {};
				for (var k in base_filter) if (Object.prototype.hasOwnProperty.call(base_filter, k)) bf[k] = base_filter[k];
				if (state.sync_timestamp) bf[modify_field] = { ge: state.sync_timestamp };
				if (page_serial !== null) bf[serial_field] = { gt: page_serial };

				var qpm = queryBuilder(config.select, bf, table);
				var urlm = getURL(server, file, table, qpm, limit);
				rowsm = await callProxy(urlm, config, token);
				if (!rowsm.length) break;

				var mappedm = applyMapping(rowsm, map_rules, types);
				var maxm = getSerial(rowsm, serial_field, page_serial);

				var rm = dispatchBatch(hooks, has_batch, mappedm, results, maxm, instance, token);
				if (isPromise(rm)) pending.push(rm);

				processed += rowsm.length;
				page_serial = maxm;
			}
			while (rowsm.length === limit);

			if (checkHalt(instance, token)) ok = false;
			if (processed === 0) { keep = false; break; }
			if (ok && odata_tokens[instance] === token)
			{
				state.sync_timestamp = cycle_start;
				saveSync(file, table, state);
			}
			if (++loops >= MODIFIED_PASS) { keep = false; break; }
		}
	}

	// Hook errors
	if (hooks && typeof hooks.onError === "function" && callProxy.error === true && odata_tokens[instance] === token)
	{
		try { hooks.onError({ stage: "fetch", error: callProxy.last_error || "Fetch error" }); } catch (_e) {}
	}

	// Recount after modified run
	if (modify_field && sync_type === "modified" && callProxy.fetch_aborted === false && callProxy.error !== true && hooks && typeof hooks.onRecount === "function" && odata_tokens[instance] === token)
	{
		var server_recount = await getCount(config, "base", token);
		try { hooks.onRecount(server_recount); } catch (_e) {}
	}

	// Clean abort notice
	if (hooks && callProxy.fetch_aborted === true && callProxy.error !== true && typeof hooks.onError === "function" && odata_tokens[instance] === token)
	{
		try { hooks.onError({ stage: "abort", error: "Fetch aborted" }); } catch (_e) {}
	}

	// Await any work returned by on batch
	if (pending.length)
	{
		try { await Promise.allSettled(pending); } catch (_e) {}
	}

	if (hooks && typeof hooks.onComplete === "function" && odata_tokens[instance] === token)
	{
		try { hooks.onComplete({ sync: sync_type, data: (has_batch ? undefined : results) }); } catch (_e) {}
	}

	return has_batch ? undefined : results;
}

// =====================================
// Unified entry point
// =====================================

async function callODATA(config)
{
	var instance = (config && config.instance) ? config.instance : "default";
	var token = Date.now().toString(36) + Math.random().toString(36).substring(2);
	odata_tokens[instance] = token;
	odata_abort = new AbortController();
	callProxy.fetch_aborted = false;
	callProxy.error = false;
	callProxy.last_error = null;

	console.time("Data retrieval");

	var server_count = 0;
	var hooks = (config && config.hooks && typeof config.hooks === "object") ? config.hooks : null;

	// Optional count
	if (hooks && typeof hooks.onCount === "function")
	{
		var probe = (config.mode && typeof config.mode.modify_field === "string" && config.mode.modify_field.trim()) ? loadSync(config.file, config.table) : null;
		var mode = (probe && probe.sync_complete === true && probe.sync_timestamp) ? "modified" : "base";
		server_count = await getCount(config, mode, token);
		if (odata_tokens[instance] === token) { try { hooks.onCount(server_count); } catch (_e) {} }
	}

	var output, pipeline_err = null;

	try
	{
		output = await getData(config, token);
	}
	catch (err)
	{
		pipeline_err = (err && err.message) ? err.message : String(err);
		if (hooks && typeof hooks.onError === "function")
		{
			try { hooks.onError({ stage: "pipeline", error: pipeline_err }); } catch (_e) {}
		}
		logError("pipeline", ERR_NET, pipeline_err);
	}
	if (odata_tokens[instance] === token) delete odata_tokens[instance];
	console.timeEnd("Data retrieval");

	return { output: output, count: server_count, error: pipeline_err };
}
