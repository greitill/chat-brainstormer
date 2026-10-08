use crate::config::Config;
use crate::models::{AnalysisResult, ChatMessage};
use serde_json::json;

pub async fn analyze_room(
    config: &Config,
    room: &str,
    messages: &[ChatMessage],
) -> AnalysisResult {
    let transcript: Vec<_> = messages
        .iter()
        .filter(|m| m.envelope.kind != "analysis")
        .map(|m| {
            json!({
                "author": m.envelope.author_name,
                "text": m.envelope.text(),
            })
        })
        .collect();

    if transcript.is_empty() {
        return AnalysisResult {
            summary: "No chat messages yet to analyze.".into(),
            themes: vec![],
            promising_ideas: vec![],
            open_questions: vec![],
            next_steps: vec!["Invite teammates and share a few rough ideas.".into()],
            source: "local".into(),
            model: None,
        };
    }

    if let Some(api_key) = &config.openai_api_key {
        match call_openai(api_key, &config.openai_model, room, &transcript).await {
            Ok(mut result) => {
                result.source = "openai".into();
                result.model = Some(config.openai_model.clone());
                return result;
            }
            Err(err) => {
                tracing::warn!("OpenAI analysis failed: {err}");
            }
        }
    }

    local_analysis(room, messages)
}

async fn call_openai(
    api_key: &str,
    model: &str,
    room: &str,
    transcript: &[serde_json::Value],
) -> Result<AnalysisResult, String> {
    let prompt = format!(
        "Analyze this brainstorming chat room '{room}'. Return ONLY JSON with keys: summary (string), themes (string[]), promising_ideas (string[]), open_questions (string[]), next_steps (string[]). Max 5 items per list.\nTranscript:\n{}",
        serde_json::to_string_pretty(transcript).unwrap_or_default()
    );

    let client = reqwest::Client::new();
    let res = client
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        // No `temperature`: reasoning models (gpt-5*) reject anything but the
        // default and the whole request 400s, silently degrading to local analysis.
        .json(&json!({
            "model": model,
            "response_format": { "type": "json_object" },
            "messages": [
                {"role": "system", "content": "You facilitate product brainstorms and return structured JSON."},
                {"role": "user", "content": prompt}
            ]
        }))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        return Err(res.text().await.unwrap_or_else(|_| "openai error".into()));
    }

    let body: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;
    let content = body["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| "empty content".to_string())?;
    let mut parsed: AnalysisResult =
        serde_json::from_str(content).map_err(|e| e.to_string())?;
    parsed.source = "openai".into();
    Ok(parsed)
}

fn local_analysis(room: &str, messages: &[ChatMessage]) -> AnalysisResult {
    let texts: Vec<&str> = messages
        .iter()
        .filter(|m| m.envelope.kind != "analysis")
        .map(|m| m.envelope.text())
        .collect();

    let mut freq = std::collections::HashMap::<String, usize>::new();
    for text in &texts {
        for word in text
            .to_lowercase()
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|w| w.len() > 4)
        {
            *freq.entry(word.to_string()).or_default() += 1;
        }
    }
    let mut themes: Vec<_> = freq.into_iter().collect();
    themes.sort_by(|a, b| b.1.cmp(&a.1));
    let themes: Vec<String> = themes.into_iter().take(5).map(|(w, _)| w).collect();

    let promising: Vec<String> = texts
        .iter()
        .filter(|t| {
            let l = t.to_lowercase();
            l.contains("idea") || l.contains("what if") || l.contains("could") || l.contains("let's")
        })
        .take(5)
        .map(|t| (*t).to_string())
        .collect();

    let questions: Vec<String> = texts
        .iter()
        .filter(|t| t.contains('?'))
        .take(5)
        .map(|t| (*t).to_string())
        .collect();

    AnalysisResult {
        summary: format!(
            "Local analysis of {} messages in {room}. Set OPENAI_API_KEY for deeper LLM insights.",
            texts.len()
        ),
        themes: if themes.is_empty() {
            vec!["brainstorm".into(), "product".into()]
        } else {
            themes
        },
        promising_ideas: if promising.is_empty() {
            texts.iter().rev().take(3).map(|t| format!("Explore: {t}")).collect()
        } else {
            promising
        },
        open_questions: if questions.is_empty() {
            vec![
                "What does success look like?".into(),
                "What should we prototype first?".into(),
            ]
        } else {
            questions
        },
        next_steps: vec![
            "Cluster top themes into 2–3 directions".into(),
            "Pick one idea for a thin prototype".into(),
            "Assign owners for open questions".into(),
        ],
        source: "local".into(),
        model: None,
    }
}
