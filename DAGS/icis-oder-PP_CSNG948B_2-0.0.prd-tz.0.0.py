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
                , WORKFLOW_NAME='PP_CSNG948B_2',WORKFLOW_ID='0c217bd500964b23b0df7261dcb504a4', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG948B_2-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 5, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0c217bd500964b23b0df7261dcb504a4')

    csng948bJob_vol = []
    csng948bJob_volMnt = []
    csng948bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng948bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc',' /app/order'))

    csng948bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng948bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng948bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng948bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c152114c3aa24e6b83b81b037fa0bc67',
        'volumes': csng948bJob_vol,
        'volume_mounts': csng948bJob_volMnt,
        'env_from':csng948bJob_env,
        'task_id':'csng948bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng948bJob", "applyDate=${YYYYMM}", "saIdSecEnum=2"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0c217bd500964b23b0df7261dcb504a4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng948bJob,
        Complete
    ]) 

    # authCheck >> csng948bJob >> Complete
    workflow








