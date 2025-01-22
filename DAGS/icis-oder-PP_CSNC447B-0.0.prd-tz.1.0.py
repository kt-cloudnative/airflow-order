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
                , WORKFLOW_NAME='PP_CSNC447B',WORKFLOW_ID='99625e51d0d04c8b8426ae290592c18e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC447B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('99625e51d0d04c8b8426ae290592c18e')

    csnc447bJob_vol = []
    csnc447bJob_volMnt = []
    csnc447bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnc447bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnc447bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc447bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc447bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc447bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6b96cc7bf45548fa9447fa715f5e6130',
        'volumes': csnc447bJob_vol,
        'volume_mounts': csnc447bJob_volMnt,
        'env_from':csnc447bJob_env,
        'task_id':'csnc447bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csnc447bJob", "requestDate="+str(datetime.now()), "workDate=202411", "saDtlCd=2342", "dcRate=1000"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('99625e51d0d04c8b8426ae290592c18e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc447bJob,
        Complete
    ]) 

    # authCheck >> csnc447bJob >> Complete
    workflow








