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
                , WORKFLOW_NAME='BI_CSAB913B',WORKFLOW_ID='439900fd4f62483cac21aadacbfe87e2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB913B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 17, 14, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('439900fd4f62483cac21aadacbfe87e2')

    csab913bJob_vol = []
    csab913bJob_volMnt = []
    csab913bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab913bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab913bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab913bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c151b00be10f44caa2ab17bfd2949fec',
        'volumes': csab913bJob_vol,
        'volume_mounts': csab913bJob_volMnt,
        'env_from':csab913bJob_env,
        'task_id':'csab913bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csab913bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('439900fd4f62483cac21aadacbfe87e2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab913bJob,
        Complete
    ]) 

    # authCheck >> csab913bJob >> Complete
    workflow








