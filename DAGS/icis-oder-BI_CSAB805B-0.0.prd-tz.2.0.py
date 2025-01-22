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
                , WORKFLOW_NAME='BI_CSAB805B',WORKFLOW_ID='228bc994027e4ebda16a6f058da3823d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB805B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 17, 12, 35, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('228bc994027e4ebda16a6f058da3823d')

    csab805bJob_vol = []
    csab805bJob_volMnt = []
    csab805bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab805bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab805bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab805bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '134126ceaaf04db5916acef905587aed',
        'volumes': csab805bJob_vol,
        'volume_mounts': csab805bJob_volMnt,
        'env_from':csab805bJob_env,
        'task_id':'csab805bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=csab805bJob", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('228bc994027e4ebda16a6f058da3823d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab805bJob,
        Complete
    ]) 

    # authCheck >> csab805bJob >> Complete
    workflow








