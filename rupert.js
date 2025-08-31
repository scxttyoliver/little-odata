// Version 2.2.0
// Removes the thread option and ensures all calls are made serialised
// Removed window call backs and provided additional hooks for greater use
// Only counts when count and recount hooks are supplied
// Hardened error logic and performs further checks for required data such as table and corrupt sync variables

// ---------------------------------------------
// Helper Function Start
// ---------------------------------------------

// ---------------------------------------------
// Globals
// ---------------------------------------------

const re_date = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
const re_number = /^-?\d+(?:\.\d+)?$/;
const odata_tokens = {};

let odata_abort = new AbortController();

// ---------------------------------------------
// Shared Helpers
// ---------------------------------------------

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

function checkHalt()
{
	return (odata_abort && odata_abort.signal && odata_abort.signal.aborted === true) || callProxy.error === true || callProxy.fetch_aborted === true;
}

function encodeField(field)
{
	return '"' + field.replace(/"/g, '""') + '"';
}

function encodeValue(val)
{
	if (val === null || val === undefined) return 'null';
	if (val instanceof Date) return val.toISOString();
	if (typeof val === 'string' && re_date.test(val)) return val;
	if (typeof val === 'string') return "'" + val.replace(/'/g, "''") + "'";
	if (typeof val !== 'number' && typeof val !== 'boolean') return "'" + String(val) + "'";
	return val;
}

function getHeaders(url, config)
{
	const proxy_url = config.proxy || "https://proxy.littleman.com.au";
	return { url: proxy_url + '/odata/get?url=' + encodeURIComponent(url), headers: { "fm-username": config.username, "fm-password": config.password } };
}

function getURL(server, file, table, query_parts, limit)
{
	const base = 'https://' + server + '/fmi/odata/v4/' + file + '/' + table;
	const params = [];

	if (query_parts.field_select) params.push('$select=' + query_parts.field_select);
	if (query_parts.field_expand) params.push(query_parts.field_expand);
	if (query_parts.field_filter) params.push(query_parts.field_filter);
	if (limit) params.push('$top=' + limit);

	return params.length ? base + '?' + params.join('&') : base;
}

function loadSync(file, table)
{
	var key = file + '-' + table + '-sync';
	try
	{
		var raw = localStorage.getItem(key);
		if (!raw) return { serial_last: null, sync_timestamp: null, sync_complete: false };

		var state = JSON.parse(raw);
		if (!state || typeof state !== 'object') throw new Error('Invalid sync state');

		if (!('serial_last' in state)) state.serial_last = null;
		if (!('sync_timestamp' in state)) state.sync_timestamp = null;
		if (!('sync_complete' in state)) state.sync_complete = false;

		return state;
	}
	catch (e)
	{
		// Self heal corrupted entry
		try { localStorage.removeItem(key); } catch (_) {}
		return { serial_last: null, sync_timestamp: null, sync_complete: false };
	}
}

function saveSync(file, table, state)
{
	localStorage.setItem(file + '-' + table + '-sync', JSON.stringify(state));
}

// ---------------------------------------------
// Query Construction
// ---------------------------------------------

function queryODATA(config_select, config_filter, table_base)
{
	function processFilter(field, condition)
	{
		const field_full = encodeField(field);
		if (Array.isArray(condition))
		{
			const clauses = condition.map(function(val){ return field_full + ' eq ' + encodeValue(val); });
			return '(' + clauses.join(' or ') + ')';
		}
		if (typeof condition === 'object' && condition !== null)
		{
			const pairs = Object.entries(condition);
			const clauses = new Array(pairs.length);
			for (var i = 0; i < pairs.length; i++)
			{
				var operator = pairs[i][0];
				var val = pairs[i][1];
				clauses[i] = operator === 'contains' ? 'contains(' + field_full + ',' + encodeValue(val) + ')' : field_full + ' ' + operator + ' ' + encodeValue(val);
			}
			return clauses.join(' and ');
		}
		return field_full + ' eq ' + encodeValue(condition);
	}

	const filter_parts = Object.entries(config_filter || {}).map(function(pair){ return processFilter(pair[0], pair[1]); });
	const filter_string = filter_parts.length ? '$filter=' + encodeURIComponent(filter_parts.join(' and ')) : '';

	var clause_select = [];
	var clause_expand = [];

	Object.keys(config_select).forEach(function (table)
	{
		const fields = Object.keys(config_select[table]).map(encodeField).join(',');
		if (table === table_base)
		{
			clause_select.push(fields);
		}
		else
		{
			clause_expand.push('"' + table + '"($select=' + fields + ')');
		}
	});

	return { field_select: encodeURIComponent(clause_select.join(',')), field_expand: clause_expand.length ? '$expand=' + clause_expand.join(',') : '', field_filter: filter_string };
}

// ---------------------------------------------
// Fetch & Mapping
// ---------------------------------------------

async function callProxy(url, config, token)
{
	const proxy = getHeaders(url, config);
	try
	{
		const instance = config.instance || "default";
		if (token && odata_tokens[instance] !== token)
		{
			console.warn("Aborting due to session token mismatch");
			return [];
		}

		const response = await fetch(proxy.url, { headers: proxy.headers, signal: odata_abort.signal });
		if (!response.ok) throw new Error("HTTPS: " + response.status);
		const data = await response.json();
		return data.value || data || [];
	}
	catch (error)
	{
		if (error && (error.name === "AbortError" || error.message === "Load failed"))
		{
			console.warn("ODATA fetch aborted by navigation or reload");
			callProxy.fetch_aborted = true;
		}
		else
		{
			console.error("ODATA fetch error:", error && error.message ? error.message : error);
			callProxy.error = true;
			callProxy.last_error = error && error.message ? error.message : String(error);
		}
		return [];
	}
}

function compileMapping(config_select, table_base)
{
	const mapping = [];
	Object.keys(config_select).forEach(function(table)
	{
		const fields = config_select[table];
		const path_source = (table === table_base) ? null : table;

		Object.keys(fields).forEach(function(field_fm)
		{
			const rule = fields[field_fm];
			const key_target = typeof rule === 'object' && rule !== null ? rule.key : rule;
			const isArray = typeof rule === 'object' && rule.isArray;
			const delimiter = typeof rule === 'object' ? (rule.delimiter || '\n') : null;

			mapping.push({ field_fm: field_fm, key_target: key_target, path_source: path_source, isArray: isArray, delimiter: delimiter });
		});
	});
	return mapping;
}

function normaliseTypes(key, value, types)
{
	if (!types) return value;

	var type_found = types[key];
	if (!type_found) return value;

	if (type_found === 'number')
	{
		if (typeof value === 'number') return value;
		if (typeof value === 'string' && value.length <= 15 && re_number.test(value)) return Number(value);
		return value;
	}

	if (type_found === 'date')
	{
		if (value instanceof Date) return value;
		if (typeof value === 'string' && re_date.test(value)) return new Date(value);
		return value;
	}

	return value;
}

function applyMapping(data, mapping_instruction, types)
{
	var len = data.length;
	var out = new Array(len);

	for (var i = 0; i < len; i++)
	{
		var record = data[i];
		var mapped = {};

		for (var j = 0, m = mapping_instruction.length; j < m; j++)
		{
			var item = mapping_instruction[j];
			var source = item.path_source
				? (Array.isArray(record[item.path_source]) ? (record[item.path_source][0] || {}) : (record[item.path_source] || {}))
				: record;

			var v = source[item.field_fm];

			if (item.isArray && typeof v === 'string')
			{
				if (item.delimiter === '\n') v = v.replace(/\r\n|\r/g, '\n');
				var parts = v.split(item.delimiter);
				var arr = [];
				for (var k = 0, p = parts.length; k < p; k++)
				{
					var s = parts[k].trim();
					if (s) arr.push(s);
				}
				mapped[item.key_target] = arr;
			}
			else
			{
				mapped[item.key_target] = (item.field_fm in source) ? normaliseTypes(item.key_target, v, types) : null;
			}
		}

		out[i] = mapped;
	}

	return out;
}

// ---------------------------------------------
// Count Query
// ---------------------------------------------

async function getCount(config, count_mode)
{
	const { table, file, server } = config;
	const instance = config.instance || "default";
	const session_token = Date.now().toString(36) + Math.random().toString(36).substring(2);
	odata_tokens[instance] = session_token;

	const count_field = (config.mode && config.mode.count_field) ? config.mode.count_field : "_Count";
	const select_fields = {};
	select_fields[table] = {};
	select_fields[table][count_field] = count_field;

	var count_filter = { ...(config.filter || {}) };

	if (count_mode === 'modified')
	{
		const sync_state = loadSync(file, table);
		if (sync_state && sync_state.sync_timestamp)
		{
			const modify_field = (config.mode && config.mode.modify_field) ? config.mode.modify_field : 'Timestamp Modify';
			count_filter[modify_field] = { ge: sync_state.sync_timestamp };
		}
	}

	const query_parts = queryODATA(select_fields, count_filter, table);
	const url = getURL(server, file, table, query_parts, 1);

	const data = await callProxy(url, config, session_token);
	delete odata_tokens[instance];
	return Array.isArray(data) && data.length > 0 ? data[0][count_field] : 0;
}

// ---------------------------------------------
// Data Query
// ---------------------------------------------

// Emit a batch either via hook or by accumulating
function dispatchBatch(hooks, hooks_batch, mapped, results, serial_last)
{
	if (hooks_batch)
	{
		const payload = (serial_last == null) ? { data: mapped, size: mapped.length } : { data: mapped, size: mapped.length, serial_last: serial_last };

		try
		{
			return hooks.onBatch(payload); 
		}
		catch (e)
		{ 
			// Ignore hook
		}
		return undefined;
	}
	else
	{
		results.push.apply(results, mapped);
		return undefined;
	}
}

async function getData(config)
{
	const { table, file, server, mode } = config;
	const instance = config.instance || "default";
	const session_token = Date.now().toString(36) + Math.random().toString(36).substring(2);
	odata_tokens[instance] = session_token;

	const limit = (mode && typeof mode.limit === 'number' && mode.limit > 0) ? (mode.limit | 0) : 10000;
	const serial_field = (mode && typeof mode.serial_field === 'string' && mode.serial_field.trim()) ? mode.serial_field.trim()	: null;
	const modify_field = (mode && typeof mode.modify_field === 'string' && mode.modify_field.trim()) ? mode.modify_field.trim() : null;
	const types = (mode && mode.types && typeof mode.types === 'object') ? mode.types : null;
	const hooks = (config && config.hooks && typeof config.hooks === 'object') ? config.hooks : null;
	var hooks_batch = (hooks && typeof hooks.onBatch === 'function');

	// Collect on batch work so we can wait before complete
	const batch_pending = [];
	function checkPromise(v){ return v && typeof v.then === 'function'; }

	// Require serial when doing modified sync
	if (modify_field && !serial_field)
	{
		if (hooks && typeof hooks.onError === 'function')
		{
			try { hooks.onError({ stage: 'config', error: 'Serial field is required for sync' }); } catch(e) {}
		}
		delete odata_tokens[instance];
		return undefined;
	}

	// Ensure base table select exists
	if (!config.select || !config.select[table] || typeof config.select[table] !== 'object')
	{
		if (hooks && typeof hooks.onError === 'function') { try { hooks.onError({ stage: 'config', error: 'Missing base table: ' + table }); } catch(e) {} }
		delete odata_tokens[instance];
		return undefined;
	}

	// Ensure serial field selected
	if (serial_field && !config.select[table][serial_field]) config.select[table][serial_field] = serial_field;

	const map_compiled = compileMapping(config.select, table);
	const base_filter = { ...(config.filter || {}) };

	var sync_state = modify_field ? loadSync(file, table) : {};
	var serial_last = (modify_field && sync_state.serial_last != null) ? Number(sync_state.serial_last) : null;
	const sync_type = modify_field ? (!sync_state.sync_complete ? 'full' : 'modified') : 'live';
	const timestamp_start = new Date();

	if (hooks && typeof hooks.onStart === 'function') { try { hooks.onStart({ sync: sync_type }); } catch(e) {} }

	// Only set sync timestamp once for full sync
	if (modify_field && sync_type === 'full' && !sync_state.sync_timestamp)
	{
		sync_state.sync_timestamp = timestamp_start;
		saveSync(file, table, sync_state);
	}

	var results = hooks_batch ? undefined : [];
	var fetched;

	// Full or live
	if (sync_type === 'full' || sync_type === 'live')
	{
		if (!serial_field)
		{
			if (!checkHalt())
			{
				var qp = queryODATA(config.select, base_filter, table);
				var url = getURL(server, file, table, qp, limit);
				var fetched = await callProxy(url, config, session_token);

				if (fetched && fetched.length)
				{
					var mapped = applyMapping(fetched, map_compiled, types);
					// Capture hook result and store promise if any
					const batch_result = dispatchBatch(hooks, hooks_batch, mapped, results, null);
					if (checkPromise(batch_result)) batch_pending.push(batch_result);
				}
			}
		}
		else
		{
			do
			{
				if (checkHalt()) break;

				var batch_filter = { ...base_filter };
				if (serial_last !== null) batch_filter[serial_field] = { gt: serial_last };

				var qp = queryODATA(config.select, batch_filter, table);
				var url = getURL(server, file, table, qp, limit);
				fetched = await callProxy(url, config, session_token);
				if (!fetched.length) break;

				var mapped = applyMapping(fetched, map_compiled, types);
				var batch_max = getSerial(fetched, serial_field, serial_last);

				// Capture hook result and store promise if any
				const batch_result = dispatchBatch(hooks, hooks_batch, mapped, results, batch_max);
				if (checkPromise(batch_result)) batch_pending.push(batch_result);

				serial_last = batch_max;

				if (modify_field)
				{
					sync_state.serial_last = serial_last;
					saveSync(file, table, sync_state);
				}
			}
			while (fetched.length === limit);

			if (modify_field && sync_type === 'full' && fetched && fetched.length < limit && callProxy.fetch_aborted === false && callProxy.error !== true)
			{
				sync_state.sync_complete = true;
				saveSync(file, table, sync_state);
			}
		}
	}

	// Modified records
	if (modify_field && sync_type === 'modified')
	{
		var keep_checking = true;
		while (keep_checking)
		{
			if (checkHalt()) break;

			var cycle_start = new Date();
			var cycle_processed = 0;
			var page_serial = null;
			var cycle_ok = true;

			do
			{
				if (checkHalt()) { cycle_ok = false; break; }

				var bf = { ...base_filter };
				if (sync_state.sync_timestamp) bf[modify_field] = { ge: sync_state.sync_timestamp };
				if (page_serial !== null) bf[serial_field] = { gt: page_serial };

				var qp_mod = queryODATA(config.select, bf, table);
				var url_mod = getURL(server, file, table, qp_mod, limit);
				fetched = await callProxy(url_mod, config, session_token);
				if (!fetched.length) break;

				var mapped_mod = applyMapping(fetched, map_compiled, types);
				var batch_max_mod = getSerial(fetched, serial_field, page_serial);

				// Capture hook result and store promise if any
				const batch_result = dispatchBatch(hooks, hooks_batch, mapped_mod, results, batch_max_mod);
				if (checkPromise(batch_result)) batch_pending.push(batch_result);

				cycle_processed += fetched.length;
				page_serial = batch_max_mod;
			}
			while (fetched.length === limit);

			if (checkHalt()) cycle_ok = false;

			// Advance only when the entire cycle completed
			if (cycle_processed === 0) { keep_checking = false; break; }
			if (cycle_ok)
			{
				sync_state.sync_timestamp = cycle_start;
				saveSync(file, table, sync_state);
			}
		}
	}

	// Error hook
	if (hooks && typeof hooks.onError === 'function' && callProxy.error === true)
	{
		try { hooks.onError({ stage: 'fetch', error: callProxy.last_error || 'ODATA fetch error' }); } catch(e) {}
	}

	// Recount hook after modified run
	if (modify_field && sync_type === 'modified' && callProxy.fetch_aborted === false && callProxy.error !== true && hooks && typeof hooks.onRecount === 'function')
	{
		const server_recount = await getCount(config, 'base');
		try { hooks.onRecount(server_recount); } catch(e) {}
	}

	delete odata_tokens[instance];

	// Notify clean aborts to hooks
	if (hooks && callProxy.fetch_aborted === true && callProxy.error !== true && typeof hooks.onError === 'function')
	{
		try { hooks.onError({ stage: 'abort', error: 'Fetch aborted' }); } catch(e) {}
	}

	// Ensure all on batch tasks are finished before
	if (batch_pending.length)
	{
		try { await Promise.allSettled(batch_pending); } catch (e) { }
	}

	if (hooks && typeof hooks.onComplete === 'function')
	{
		try { hooks.onComplete({ sync: sync_type, data: (hooks_batch ? undefined : results) }); } catch(e) {}
	}

	return hooks_batch ? undefined : results;
}

// ---------------------------------------------
// Unified Entry Point
// ---------------------------------------------

async function callODATA(config)
{
	odata_abort = new AbortController();
	callProxy.fetch_aborted = false;
	callProxy.error = false;
	callProxy.last_error = null;

	console.time('ODATA Retrieval');

	var server_count = 0;
	var hooks = (config && config.hooks && typeof config.hooks === 'object') ? config.hooks : null;

	// If caller requires count
	if (hooks && typeof hooks.onCount === 'function')
	{
		var sync_probe = (config.mode && typeof config.mode.modify_field === 'string' && config.mode.modify_field.trim()) ? loadSync(config.file, config.table) : null;
		var count_mode = (sync_probe && sync_probe.sync_complete === true && sync_probe.sync_timestamp) ? 'modified' : 'base';
		server_count = await getCount(config, count_mode);
		try { hooks.onCount(server_count); } catch(e) {}
	}

	var data, error_in_call = null;

	try
	{
		data = await getData(config);
	}
	catch (err)
	{
		error_in_call = err && err.message ? err.message : String(err);
		if (hooks && typeof hooks.onError === 'function')
		{
			try { hooks.onError({ stage: 'pipeline', error: error_in_call }); } catch(e) {}
		}
	}

	console.timeEnd('ODATA Retrieval');

	return { output: data, count: server_count, error: error_in_call };
}

// ==============================
// Helper Function Stop
// ==============================
