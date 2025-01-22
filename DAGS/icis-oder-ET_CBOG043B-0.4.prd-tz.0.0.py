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
                , WORKFLOW_NAME='ET_CBOG043B',WORKFLOW_ID='bd06c4fbdf3b40fd9bdbd5f33fdf89f3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG043B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bd06c4fbdf3b40fd9bdbd5f33fdf89f3')

    cbog043bJob_vol = []
    cbog043bJob_volMnt = []
    cbog043bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog043bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog043bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog043bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog043bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog043bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89f50f0edc2245e8a6d4d9f298c384bf',
        'volumes': cbog043bJob_vol,
        'volume_mounts': cbog043bJob_volMnt,
        'env_from':cbog043bJob_env,
        'task_id':'cbog043bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.6',
        'arguments':["--job.name=cbog043bJob", "reqYm=${YYYYMMDD}", "mobileFlag=0", "procResultFlag=S", "regEmpNo=82000009", "regOfcCd=712616", "regEmpName=cbog043b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bd06c4fbdf3b40fd9bdbd5f33fdf89f3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog043bJob,
        Complete
    ]) 

    # authCheck >> cbog043bJob >> Complete
    workflow








