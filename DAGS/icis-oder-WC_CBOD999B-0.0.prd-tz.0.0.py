from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='WC_CBOD999B',WORKFLOW_ID='ab51cd20d3864683a1b12943cf54ceda', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOD999B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 31, 16, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ab51cd20d3864683a1b12943cf54ceda')

    cbod999bJob_vol = []
    cbod999bJob_volMnt = []
    cbod999bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbod999bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod999bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod999bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd1ea965a7463469788ccc9ecccc61208',
        'volumes': cbod999bJob_vol,
        'volume_mounts': cbod999bJob_volMnt,
        'env_from':cbod999bJob_env,
        'task_id':'cbod999bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.25',
        'arguments':["--job.name=cbod999bJob", "pgmNm=cbod999b", "endTranDate=${YYYYMMDD}", "empNo=091015557", "workDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ab51cd20d3864683a1b12943cf54ceda')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod999bJob,
        Complete
    ]) 

    # authCheck >> cbod999bJob >> Complete
    workflow








