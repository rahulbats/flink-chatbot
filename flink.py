import requests
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain.chat_models import ChatOpenAI, ChatOllama
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import StrOutputParser
import time
import streamlit as st
import json
import traceback
import schedule

#load_dotenv()

 

if(st.session_state.get("authentication_status") is None):
    st.session_state.authentication_status=False

def changeModel():
    if(st.session_state.get("llmProvider")=="gpt3"):
        st.session_state.llm=ChatOpenAI(temperature=0,openai_api_key=st.session_state.llm_key, model_name="gpt-3.5-turbo")
    elif(st.session_state.get("llmProvider")=="gpt4"):
        st.session_state.llm=ChatOpenAI(temperature=0,openai_api_key=st.session_state.llm_key, model_name="gpt-4-turbo-preview")
    elif(st.session_state.get("llmProvider")=="claude"):
        st.session_state.llm = ChatAnthropic(temperature=0, anthropic_api_key=st.session_state.llm_key, model_name="claude-3-opus-20240229")
       
  

    st.session_state.authentication_status=True
    st.sidebar.success("You have successfully authenticated", icon='✅')


def submit():
            st.session_state.schema = None
            st.session_state.submitted = True
            

 
  
with st.sidebar:
    with st.form("llm_form"):
        confluentCloudProvider=st.selectbox("LLM provider", ["gpt3", "gpt4", "claude"], key="llmProvider")  
        llmKey =st.text_input("LLM Key", type="password", key="llm_key")
        submit_llm_button = st.form_submit_button("Submit", on_click=changeModel)

    with st.form("flink_form"):
                st.title('Confluent Configuration details')
                
                
                confluentApiKey = st.text_input("Confluent Flink API Key", key="confluentApiKey")
                confluentApiSecret= st.text_input("Confluent Flink API Secret", type="password", key="confluentApiSecret")
                confluentEnvironment= st.text_input("Confluent Environment ID", key="confluentEnvironment")
                confluentOrg=st.text_input("Confluent Organization ID", key="confluentOrg")
                confluentPool=st.text_input("Confluent Flink Compute Pool", key="confluentPool")
                confluentPrincipal=st.text_input("Confluent Flink Principal", key="confluentPrincipal")
                confluentCatalog=st.text_input("Confluent Flink Environment Name", key="confluentCatalog")
                confluentDatabase=st.text_input("Confluent Kafka Cluster Name(Flink DB)", key="confluentDatabase")
                confluentCloudProvider=st.selectbox("Cloud provider", ["azure", "aws", "gcp"], key="confluentCloudProvider")
                confluentCloudRegion=st.text_input("Cloud provider region", key="confluentCloudRegion")
                submit_button = st.form_submit_button("Submit", on_click=submit)                  
        
              
             
st.image("confluent-logo.png", width=50)       
if(st.session_state.get("authentication_status") is False):
     st.title("Flink SQL- A chatbot for Flink SQL")
     st.subheader("Please fill in the form to authenticate")
else:
    st.title("Flink SQL- A chatbot for Flink SQL")
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "I am a chatbot which can analyze and query your flink tables?"}]



template = """Based on the table schema below, write a flink SQL query that would answer the user's question, limit the query to 10 rows if the query does not have it.Creation timestamp is represented by a hidden row $rowtime which is of datatype TIMESTAMP_LTZ(3). So if you want to compare it to a string you are sending , you will have to cast the string to TIMESTAMP_LTZ(3) using cast(timestamp TIMESTAMPSTRING as TIMESTAMP_LTZ(3)). Use Flink's new Windowing Table Valued Functions to answer questions.  Don't use columns in where condition if the table does not have the column. Dont use alias names which are keywords like count. Use the exact table names from schema. Your answer should be just the sql query and nothing else. if you dont know the answer, just say "I dont know".
{schema}

Question: {question}
SQL Query:"""



sqlprompt = ChatPromptTemplate.from_template(template)


def getFlinkStatementsURL():
    return f"https://flink.{confluentCloudRegion}.{confluentCloudProvider}.confluent.cloud/sql/v1beta1/organizations/{confluentOrg}/environments/{confluentEnvironment}/statements"


@st.cache_data(show_spinner=False)
def get_schema():


    requests.delete(getFlinkStatementsURL()+"/show-tables",auth=(confluentApiKey, confluentApiSecret))
        
    result = requests.post(getFlinkStatementsURL(),
                json= {
    'name': 'show-tables',
    'organization_id': confluentOrg,
    'environment_id': confluentEnvironment,
    'spec': {
    'statement': 'show tables',
    'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
    'compute_pool_id': confluentPool,
    'principal': confluentPrincipal,
    'stopped': False
    }}, auth=(confluentApiKey, confluentApiSecret))

    time.sleep(10)
    results  = requests.get(getFlinkStatementsURL()+"/show-tables/results",auth=(confluentApiKey, confluentApiSecret))
    print(results.json())
    resultTables = [ x.get('row')[0] for x in results.json().get('results').get('data')]

    schema = ""
    
    for table in resultTables:
        tableNameinURL=str.replace(table, '.', '-').replace('_','-')
        result = requests.post(getFlinkStatementsURL(),
                json= {
                    'name': 'show-create-'+tableNameinURL,
                    'organization_id': confluentOrg,
                    'environment_id': confluentEnvironment,
                    'spec': {
                        'statement': f'show create table `{table}`',
                        'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
                        'compute_pool_id': confluentPool,
                        'principal': confluentPrincipal,
                        'stopped': False
                    }
                    }, auth=(confluentApiKey, confluentApiSecret))
        if(result.json().get('status_code')!=200):
            
            time.sleep(10)
            resultschema = requests.get(getFlinkStatementsURL()+"/show-create-"+tableNameinURL+"/results",auth=(confluentApiKey, confluentApiSecret))
            schema+=resultschema.json().get('results').get('data')[0].get('row')[0]
        else:
             print(result.json())

        requests.delete(getFlinkStatementsURL()+"/show-create-"+tableNameinURL,auth=(confluentApiKey, confluentApiSecret))
    print(schema)
    
    
    
    return schema


def create_statement(query):
    
        requests.delete(getFlinkStatementsURL()+"/user-flink-query",auth=(confluentApiKey, confluentApiSecret))
        result = requests.post(getFlinkStatementsURL(),
                json= {
                        'name': 'user-flink-query',
                        'organization_id': confluentOrg,
                        'environment_id': confluentEnvironment,
                        'spec': {
                        'statement': query,
                        'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
                        'compute_pool_id': confluentPool,
                        'principal': confluentPrincipal,
                        'stopped': False
                        }}, auth=(confluentApiKey, confluentApiSecret))
        return result
    

def run_query(query, st):
    tableplaceholder = st.empty()

    try:
        time.sleep(10)
        results =requests.get(getFlinkStatementsURL()+"/user-flink-query",auth=(confluentApiKey, confluentApiSecret))
        columnsJson=results.json().get('status').get('result_schema').get('columns')
        columns=[x.get('name') for x in columnsJson]
        print(columns)
        results =requests.get(getFlinkStatementsURL()+"/user-flink-query/results?page_size=100",auth=(confluentApiKey,confluentApiSecret))
        print(results.json())
        data=results.json().get('results').get('data')
        next=results.json().get('metadata').get('next')
        counter=0
        dataRows=[]  
        while next is not None and next !="" and st.session_state.messages[-1]["role"] == "assistant" and st.session_state.get("query_running") is True:
            print("trying next page"+next)
            results =requests.get(next,auth=(confluentApiKey, confluentApiSecret))
            if(results.status_code==200):
                data=results.json().get('results').get('data')
                
                next=results.json().get('metadata').get('next')
                
                if data is not None and len(data) > 0:
                    for dataRow in data:
                        op = dataRow.get('op')
                        if op == 0:
                                dataRows.append(dataRow.get('row'))
                        elif op == 1:
                                dataRows.pop()
                        elif op ==  2:
                                dataRows.append(dataRow.get('row'))        
                        elif op ==  3:
                                dataRows.pop()
                        else:
                                dataRows.append(dataRow.get('row'))
                content = convertToHTMLTable(columns, dataRows[-10:])
                tableplaceholder.empty()   
                tableplaceholder.markdown("Following is the response for your query.</br>"+content,  unsafe_allow_html=True)
                       
                            
        return {"columns":columns, "data":dataRows, "next":next}
    except Exception as e:
        print(f"got execption for query {query}")
        print(traceback.format_exc())
        st.session_state.query_running=False
        tableplaceholder.empty()   
        tableplaceholder.markdown("Sorry I could not retrieve the data for your query.The query i generated was wrong")
        return {"columns":None, "data":None }   
    
def convertToHTMLTable(columns, data):
    if data is None or len(data)==0:
        return "No data found"
    else:
        html="<table><tr>"
        for column in columns:
            html+="<th>"+column+"</th>"
        html+="</tr>"
        for row in data:
            html+="<tr>"
            for cell in row:
                html+="<td>"+str(cell)+"</td>"
            html+="</tr>"
        html+="</table>"
        return html



           
def stop_query():
     st.session_state.query_running=False
     requests.delete(getFlinkStatementsURL()+"/user-flink-query",auth=(confluentApiKey, confluentApiSecret))

def refresh():
     get_schema.clear()
     st.session_state.schema = None
     #st.session_state.submitted = True


if(st.session_state.get("submitted") is True):  
    if 'schema' not in st.session_state:
        st.session_state.schema = None

    llm = st.session_state.llm
    sql_chain = (
        sqlprompt
        | llm.bind(stop=["\nSQL"])
        | StrOutputParser()
    )


    if st.session_state.get("schema") is None:
        with st.chat_message("assistant"):
            with st.spinner("Analyzing schema..."):
                st.session_state.promptDisabled= True
                st.session_state.schema=get_schema()
                st.session_state.messages.append({"role": "assistant", "content": "I finished analyzing your tables, you can now ask me any question"})
                st.markdown("I finished analyzing your tables, you can now ask me any question")
                st.button("Refresh schema", key="refresh", on_click=refresh )  
                st.session_state.promptDisabled= False
    else:   
        with st.chat_message("assistant"): 
            st.markdown("I finished analyzing your tables, you can now ask me any question")
            st.button("Refresh schema", key="refresh", on_click=refresh )    
            st.session_state.promptDisabled= False

    if prompt:=st.chat_input("Enter your question",  disabled=st.session_state.promptDisabled):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

    if st.session_state.messages[-1]["role"] != "assistant":
        st.session_state.query_running= True
        with st.chat_message("assistant"):
            with st.spinner("Figuring out query..."):
                if(st.session_state.get("query_running") is True):
                    st.button("Stop", key="stop_query", on_click=stop_query)
                query =sql_chain.invoke({"schema":st.session_state.get("schema"), "question":prompt})
                query=query.replace("```sql\n","").replace("\n```","")

            with st.spinner("Continually Retrieving data, to stop click on Stop..."):        
                st.session_state.messages.append({"role": "assistant", "content": "result"}) 
                st.markdown("query:`"+query+"`", unsafe_allow_html=False)      
                create_statement(query)
                run_query(query, st)
                
        