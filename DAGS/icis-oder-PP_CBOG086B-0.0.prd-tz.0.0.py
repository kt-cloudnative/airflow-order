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
                , WORKFLOW_NAME='PP_CBOG086B',WORKFLOW_ID='41e777f86d034bfd8a6e6aea0b249fe0', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG086B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('41e777f86d034bfd8a6e6aea0b249fe0')

    cbog086bJob_vol = []
    cbog086bJob_volMnt = []
    cbog086bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog086bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog086bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog086bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog086bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog086bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c61a8cbc43fe4e7c80e59da98c931755',
        'volumes': cbog086bJob_vol,
        'volume_mounts': cbog086bJob_volMnt,
        'env_from':cbog086bJob_env,
        'task_id':'cbog086bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog086bJob","fromDate=20070215" ,"toDate=20070222", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('41e777f86d034bfd8a6e6aea0b249fe0')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog086bJob,
        Complete
    ]) 

    # authCheck >> cbog086bJob >> Complete
    workflow








